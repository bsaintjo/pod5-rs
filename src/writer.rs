//! Writing POD5 files
//!
//! Provides an interface for writing dataframes as POD5 tables.
use std::{
    collections::HashSet,
    io::{Seek, Write},
    marker::PhantomData,
    sync::Arc,
};

use polars::{
    error::PolarsError,
    frame::DataFrame,
    prelude::{ArrowField, CompatLevel},
};
use polars_arrow::{datatypes::Metadata, io::ipc::write::FileWriter, record_batch::RecordBatch};
use polars_schema::Schema;
use uuid::Uuid;

use crate::{
    dataframe::{
        compatibility::{
            field_arrs_to_record_batch, field_arrs_to_schema, record_batch_to_compat,
            series_to_array,
        },
        ReadDataFrame, RunInfoDataFrame, SignalDataFrame,
    },
    footer::footer_generated::minknow::reads_format::{
        ContentType, EmbeddedFile, EmbeddedFileArgs, Footer, FooterArgs,
    },
    FILE_SIGNATURE,
};

const SOFTWARE: &str = "pod5-rs";
const POD5_VERSION: &str = "0.0.40";
const FOOTER_MAGIC: [u8; 8] = [b'F', b'O', b'O', b'T', b'E', b'R', 0x000, 0x000];

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("Failed to write POD5 signature: {0}")]
    FailedToWriteSignature(std::io::Error),

    #[error("Failed to write POD5 signature: {0}")]
    FailedToWriteSectionMarker(std::io::Error),

    #[error("Failed to write POD5 footer magic: {0}")]
    FailedToWriteFooterMagic(std::io::Error),

    #[error("Failed to write POD5 footer: {0}")]
    FailedToWriteFooter(std::io::Error),

    #[error("Writer: Polars error: {0}")]
    PolarsError(#[from] PolarsError),

    #[error("Writer: stream position error: {0}")]
    StreamPositionError(std::io::Error),

    #[error("Writer: content type not other and already written: {0:?}")]
    ContentTypeAlreadyWritten(ContentType),

    #[error("Writer: Failed to rewind underlying writer: {0}")]
    FailedToRewind(std::io::Error),

    #[error("Writer: Failed to write footer length as bytes")]
    FailedToWriteFooterLengthBytes,
}

pub struct TableBatch {
    batch: RecordBatch,
    schema: Arc<Schema<ArrowField>>,
}

pub trait IntoTable {
    fn into_record_batch(self) -> Result<TableBatch, PolarsError>;
    fn as_dataframe(&self) -> &DataFrame;
    fn content_type() -> ContentType {
        ContentType::OtherIndex
    }
}

impl<'a, T> IntoTable for &'a T
where
    T: IntoTable,
{
    fn into_record_batch(self) -> Result<TableBatch, PolarsError> {
        todo!()
    }

    fn as_dataframe(&self) -> &DataFrame {
        todo!()
    }
}

impl IntoTable for SignalDataFrame {
    fn as_dataframe(&self) -> &DataFrame {
        &self.0
    }

    fn into_record_batch(self) -> Result<TableBatch, PolarsError> {
        _into_record_batch(&self.0)
    }

    fn content_type() -> ContentType {
        ContentType::SignalTable
    }
}

impl IntoTable for ReadDataFrame {
    fn as_dataframe(&self) -> &DataFrame {
        &self.0
    }

    fn into_record_batch(self) -> Result<TableBatch, PolarsError> {
        _into_record_batch(&self.0)
    }

    fn content_type() -> ContentType {
        ContentType::ReadsTable
    }
}
impl IntoTable for RunInfoDataFrame {
    fn as_dataframe(&self) -> &DataFrame {
        &self.0
    }

    fn into_record_batch(self) -> Result<TableBatch, PolarsError> {
        _into_record_batch(&self.0)
    }

    fn content_type() -> ContentType {
        ContentType::RunInfoTable
    }
}

fn _into_record_batch(df: &polars::prelude::DataFrame) -> Result<TableBatch, PolarsError> {
    let field_arrays = df
        .iter()
        .map(|s| series_to_array(s.clone()))
        .collect::<Vec<_>>();
    let schema = Arc::new(field_arrs_to_schema(&field_arrays));
    let chunk = field_arrs_to_record_batch(field_arrays, schema.clone());
    Ok(TableBatch {
        batch: chunk,
        schema,
    })
}
struct TableInfo {
    offset: i64,
    length: i64,
    content_type: ContentType,
}

#[derive(Debug, Clone)]
struct WriteOptions {
    allow_overwrite: bool,
}

pub struct Writer<W>
where
    W: Write + Seek,
{
    position: u64,
    writer: W,
    section_marker: Uuid,
    file_identifier: Uuid,
    tables: Vec<TableInfo>,
    contents_writtens: HashSet<ContentType>,
    footer_written: bool,
    metadata: Arc<Metadata>,
}

impl<W: Write + Seek> Writer<W> {
    pub(crate) fn new(writer: W) -> Self {
        let section_marker = Uuid::new_v4();
        let file_identifier = Uuid::new_v4();
        let mut metadata = Metadata::new();
        metadata.insert("MINKNOW:pod5_version".into(), POD5_VERSION.into());
        metadata.insert("MINKNOW:software".into(), SOFTWARE.into());
        metadata.insert(
            "MINKNOW:file_identifier".into(),
            file_identifier.to_string().into(),
        );
        let metadata = Arc::new(metadata);
        Self {
            position: 0,
            writer,
            section_marker,
            tables: Vec::new(),
            contents_writtens: HashSet::new(),
            footer_written: false,
            file_identifier,
            metadata,
        }
    }

    pub(crate) fn into_inner(self) -> W {
        self.writer
    }

    pub(crate) fn write_signature(&mut self) -> Result<(), WriteError> {
        self.writer
            .write_all(&FILE_SIGNATURE)
            .map_err(WriteError::FailedToWriteSignature)?;
        Ok(())
    }

    pub(crate) fn write_section_marker(&mut self) -> Result<(), WriteError> {
        self.writer
            .write_all(self.section_marker.as_bytes())
            .map_err(WriteError::FailedToWriteSectionMarker)?;
        Ok(())
    }

    /// Build and intialize a new POD5 Writer.
    ///
    /// This will write the POD5 signature and section marker to the file.
    /// SAFETY: This will rewind the underlying writer, so it will be safe
    /// writing to a file that has already been written to.
    pub fn from_writer(mut writer: W) -> Result<Self, WriteError> {
        writer.rewind().map_err(WriteError::FailedToRewind)?;
        let mut w = Self::new(writer);
        w.init()?;
        Ok(w)
    }

    pub(crate) fn init(&mut self) -> Result<(), WriteError> {
        self.write_signature()?;
        self.write_section_marker()?;
        self.position = self
            .writer
            .stream_position()
            .map_err(WriteError::StreamPositionError)?;
        Ok(())
    }

    /// Write a table to the POD5 file using a write guard. At the end of the
    /// closure, the section marker is written to the file, and the Writer
    /// is updated with the written content type.
    ///
    /// This is primarily useful for writing multiple tables correctly. If you
    /// only need to write a single table, use the `write_table` method.
    ///
    /// This method will return an error if the content type is not `OtherIndex`
    /// and the content type has already been written. This is to prevent
    /// writing multiple tables of the same type to the file.
    pub fn write_tables_with<F, T>(&mut self, inserter: F) -> Result<(), WriteError>
    where
        F: FnMut(&mut TableWriteGuard<W, T>) -> Result<(), WriteError>,
        T: IntoTable,
    {
        self._write_tables_with(inserter)?;
        // let new_content = T::content_type();
        // if new_content != ContentType::OtherIndex &&
        // self.contents_writtens.contains(&new_content) {     return
        // Err(WriteError::ContentTypeAlreadyWritten(new_content)); }
        // inserter(&mut TableWriteGuard::new(self))?;
        self.end_table(T::content_type())?;
        Ok(())
    }

    fn _write_tables_with<F, T>(&mut self, mut inserter: F) -> Result<(), WriteError>
    where
        F: FnMut(&mut TableWriteGuard<W, T>) -> Result<(), WriteError>,
        T: IntoTable,
    {
        let new_content = T::content_type();
        if new_content != ContentType::OtherIndex && self.contents_writtens.contains(&new_content) {
            return Err(WriteError::ContentTypeAlreadyWritten(new_content));
        }
        let mut guard = self.write_guard();
        // inserter(&mut TableWriteGuard::new(self))?;
        inserter(&mut guard)?;
        guard.finish()?;
        Ok(())
    }

    fn write_guard<T: IntoTable>(&mut self) -> TableWriteGuard<'_, W, T> {
        TableWriteGuard::new(self)
    }

    /// Write a single table to the POD5 file. The writer is updated with
    ///
    /// This is a convienence function for writing a single table to the file.
    /// If you need to write multiple tables, use the `write_tables_with`
    /// method.
    ///
    /// Similar to writing with the `write_tables_with` method, this will return
    /// an error if the content type is not `OtherIndex` and the content
    /// type has already been written.
    pub fn write_table<D: IntoTable>(&mut self, df: &D) -> Result<(), WriteError> {
        self.write_tables_with(|guard| guard.write_table(df))
    }

    pub fn write_table_iter<'a, I, D>(&mut self, mut iter: I) -> Result<(), WriteError>
    where
        I: Iterator<Item = &'a D>,
        D: IntoTable + 'a,
    {
        self.write_tables_with(|guard| {
            for df in iter.by_ref() {
                guard.write_table(&df)?;
            }
            Ok(())
        })
    }

    fn end_table(&mut self, content_type: ContentType) -> Result<(), WriteError> {
        let new_position = self
            .writer
            .stream_position()
            .map_err(WriteError::StreamPositionError)?;
        let padding = 8 - (new_position % 8);
        self.write_all(&vec![0u8; padding as usize]).unwrap();
        self.write_section_marker().unwrap();
        let offset = self.position as i64;
        let length = new_position as i64 - self.position as i64;
        self.tables.push(TableInfo {
            offset,
            length,
            content_type,
        });
        self.position = self.writer.stream_position().unwrap();
        Ok(())
    }

    /// Write the flatbuffers footer and last signature bits to finish writing
    /// the file.
    pub fn finish(mut self) -> Result<(), WriteError> {
        self._finish()?;
        Ok(())
    }

    pub(crate) fn _finish(&mut self) -> Result<(), WriteError> {
        self.write_footer_magic()?;
        self.write_footer()?;
        self.write_section_marker()?;
        self.write_signature()?;
        Ok(())
    }

    fn write_footer_magic(&mut self) -> Result<(), WriteError> {
        self.writer
            .write_all(&FOOTER_MAGIC)
            .map_err(WriteError::FailedToWriteFooterMagic)?;
        Ok(())
    }

    fn write_footer(&mut self) -> Result<u64, WriteError> {
        let footer = build_footer2(&self.file_identifier, &self.tables);
        // let padding_len = footer.len() % 8;
        // footer.extend_from_slice(&vec![0; padding_len]);
        self.writer
            .write_all(&footer)
            .map_err(WriteError::FailedToWriteFooter)?;
        let footer_len_bytes = (footer.len() as i64).to_le_bytes();
        self.writer
            .write_all(&footer_len_bytes)
            .map_err(|_| WriteError::FailedToWriteFooterLengthBytes)?;
        Ok(footer.len() as u64)
    }

    fn write_dataframe<D: IntoTable>(&mut self, df: &D) -> Result<(), WriteError> {
        let batch = df
            .as_dataframe()
            .iter_chunks(CompatLevel::newest(), false)
            .next()
            .unwrap();
        let batch = record_batch_to_compat(batch).unwrap();
        let schema = Arc::new(batch.schema().clone());

        let mut writer =
            FileWriter::try_new(&mut self.writer, schema, None, Default::default()).unwrap();
        writer.write(&batch, None).unwrap();
        writer.finish().unwrap();
        self.end_table(D::content_type())?;
        Ok(())
    }

    fn write_and_guard<D: IntoTable>(
        &mut self,
        df: &D,
    ) -> Result<TableWriteGuard<'_, W, D>, WriteError> {
        let batch = df
            .as_dataframe()
            .iter_chunks(CompatLevel::newest(), false)
            .next()
            .unwrap();
        let batch = record_batch_to_compat(batch).unwrap();
        let schema = Arc::new(batch.schema().clone());

        let mut writer = FileWriter::try_new(self, schema, None, Default::default()).unwrap();
        writer.write(&batch, None).unwrap();
        Ok(TableWriteGuard::from_file_writer(writer))
    }
}

impl<W: Write + Seek> Write for Writer<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

fn build_footer2(file_identifier: &Uuid, table_infos: &[TableInfo]) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let mut tables = Vec::with_capacity(table_infos.len());
    for table in table_infos {
        let efile_args = EmbeddedFileArgs {
            offset: table.offset,
            length: table.length,
            content_type: table.content_type,
            ..Default::default()
        };
        let efile = EmbeddedFile::create(&mut builder, &efile_args);
        tables.push(efile);
    }
    let contents = Some(builder.create_vector(&tables));

    let file_identifier = Some(builder.create_string(&file_identifier.to_string()));
    let software = Some(builder.create_string(SOFTWARE));
    let pod5_version = Some(builder.create_string(POD5_VERSION));

    let fbtable = Footer::create(
        &mut builder,
        &FooterArgs {
            file_identifier,
            software,
            pod5_version,
            contents,
        },
    );

    builder.finish_minimal(fbtable);
    builder.finished_data().to_vec()
}

/// When starting to write an Arrow IPC file, there are a few things that need
/// to be set up and can only be called once.
/// 1) initialize the polars_arrow FileWriter
/// 2) Set the custom metadata
/// 3) call FileWriter::start
///
/// Order to do #1, we need to have the Schema for the table we want to write.
/// Either A) we need to know it ahead of time, which is possible since the
/// TableWriteGuard has a T enforce writing the same DataFrame. We would need to
/// store the schemas for regular POD5 files for the non-OtherIndex tables.
/// B) Use the TableWriter enum  as below. On first pass, before things have
/// been initialized We do #1, #2, #3, and change to the PostInit value.
enum TableWriter<'a, W>
where
    W: Write + Seek,
{
    PreInit(&'a mut Writer<W>),
    PostInit(FileWriter<&'a mut Writer<W>>),
}

/// An scoped guard for writing a specific table type to the POD5 file.
/// This allows for writing a table iteratively, and ensures that only one of
/// each non-OtherIndex tables have been written.
///
/// To get a TableWriteGuard, use the `write_table_with` method on the Writer.
///
/// When this guard is dropped, the section marker is written to the file, and
/// Writer is updated with info about the content type of table, preventing
/// future writes of the same table type.
pub struct TableWriteGuard<'a, W, T>
where
    W: Write + Seek,
    T: IntoTable,
{
    inner: Option<TableWriter<'a, W>>,
    metadata: Arc<Metadata>,
    table: PhantomData<T>,
}

impl<'a, W, T> TableWriteGuard<'a, W, T>
where
    W: Write + Seek,
    T: IntoTable,
{
    fn new(inner: &'a mut Writer<W>) -> Self {
        let mut metadata = Metadata::new();
        metadata.insert("MINKNOW:pod5_version".into(), POD5_VERSION.into());
        metadata.insert("MINKNOW:software".into(), SOFTWARE.into());
        metadata.insert(
            "MINKNOW:file_identifier".into(),
            inner.file_identifier.to_string().into(),
        );
        Self {
            inner: Some(TableWriter::PreInit(inner)),
            table: PhantomData,
            metadata: Arc::new(metadata),
        }
    }

    fn from_file_writer(writer: FileWriter<&mut Writer<W>>) -> TableWriteGuard<'_, W, T> {
        let mut metadata = Metadata::new();
        metadata.insert("MINKNOW:pod5_version".into(), POD5_VERSION.into());
        metadata.insert("MINKNOW:software".into(), SOFTWARE.into());
        metadata.insert("MINKNOW:file_identifier".into(), "test".into());
        TableWriteGuard {
            inner: Some(TableWriter::PostInit(writer)),
            metadata: Arc::new(metadata),
            table: PhantomData,
        }
    }

    // pub fn init_take<F>(&mut self, f: F) -> Result<(), WriteError> where F:
    // Fn(&mut Writer<W>) {     let mut w = match self.inner.take() {
    //         Some(TableWriter::PreInit(writer)) => {
    //             let mut writer = FileWriter::new(writer, schema, None,
    // Default::default());
    // writer.set_custom_schema_metadata(self.metadata.clone());
    // writer.start()?;             writer
    //         }
    //         Some(TableWriter::PostInit(writer)) => writer,
    //         None => unreachable!(),
    //     };
    //     Ok(())
    // }

    pub fn write_table(&mut self, df: &T) -> Result<(), WriteError> {
        let batches = df
            .as_dataframe()
            .iter_chunks(CompatLevel::newest(), false)
            .collect::<Vec<_>>();
        let schema = Arc::new(batches[0].schema().clone());

        let mut w = match self.inner.take() {
            Some(TableWriter::PreInit(writer)) => {
                let mut writer = FileWriter::new(writer, schema, None, Default::default());
                writer.set_custom_schema_metadata(self.metadata.clone());
                writer.start()?;
                writer
            }
            Some(TableWriter::PostInit(writer)) => writer,
            None => {
                panic!("Shouldn't be able to get here")
            }
        };
        for chunk in batches.into_iter() {
            let chunk = record_batch_to_compat(chunk).unwrap();
            w.write(&chunk, None)?;
        }
        self.inner = Some(TableWriter::PostInit(w));
        Ok(())
    }

    fn finish(mut self) -> Result<(), WriteError> {
        if let Some(TableWriter::PostInit(mut x)) = self.inner.take() {
            x.finish()?;
        }
        Ok(())
    }
    fn finish2(mut self) -> Result<(), WriteError> {
        if let Some(TableWriter::PostInit(mut x)) = self.inner.take() {
            x.finish()?;
            let inner = x.into_inner();
            inner.end_table(T::content_type())?;
        }
        Ok(())
    }
}

// impl<W, T> Drop for TableWriteGuard<'_, W, T>
// where
//     W: Write + Seek,
//     T: IntoTable,
// {
//     fn drop(&mut self) {
//         if let Some(TableWriter::PostInit(mut x)) = self.inner.take() {
//             x.finish().unwrap();
//         } else {
//             // writer is always Some
//             unreachable!()
//         }
//     }
// }

#[cfg(test)]
mod test {
    use std::{fs::File, io::Cursor};

    use polars::{
        df,
        prelude::{CategoricalChunkedBuilder, CategoricalOrdering, CompatLevel},
        series::{IntoSeries, Series},
    };
    use polars_arrow::io::ipc::read::{read_file_metadata, FileReader};

    use super::*;
    use crate::{
        dataframe::{compatibility::record_batch_to_compat, get_next_df},
        reader::Reader,
    };

    #[test]
    fn test_writer_dict_roundtrip() {
        let mut dict_column =
            CategoricalChunkedBuilder::new("test".into(), 3, CategoricalOrdering::Physical);
        dict_column.append_value("alpha");
        dict_column.append_value("beta");
        dict_column.append_null();
        dict_column.register_value("gamma");
        let dict_column = dict_column.finish();
        let dict_column = dict_column.into_series().into_frame();
        let schema = dict_column
            .iter_chunks(CompatLevel::newest(), false)
            .next()
            .unwrap()
            .schema()
            .clone();
        let fields = schema.iter().map(|x| x.1).cloned().collect::<Vec<_>>();
        let buf = Cursor::new(Vec::new());
        let mut writer = Writer::new(buf);
        writer.init().unwrap();
        let fst = writer.writer.stream_position().unwrap();
        writer
            ._write_tables_with(|guard| guard.write_table(&SignalDataFrame(dict_column.clone())))
            .unwrap();
        let snd = writer.writer.stream_position().unwrap();
        writer.write_section_marker().unwrap();
        let pos = writer.writer.stream_position().unwrap();
        writer
            ._write_tables_with(|guard| guard.write_table(&ReadDataFrame(dict_column.clone())))
            .unwrap();

        let buf = writer.into_inner().into_inner();
        let new_buf = buf[fst as usize..snd as usize].to_vec();
        let mut buf = Cursor::new(new_buf);
        // let mut buf = writer.into_inner();
        // buf.rewind().unwrap();
        // buf.seek(std::io::SeekFrom::Start(6)).unwrap();
        let metadata = read_file_metadata(&mut buf).unwrap();

        let mut reader = FileReader::new(buf, metadata, None, None);
        let _ = reader.next().unwrap().unwrap();
        // let _ = get_next_df(&fields, &mut reader);
    }

    #[test]
    fn test_dictionary_df_roundtrip2() {
        let mut dict_column =
            CategoricalChunkedBuilder::new("test".into(), 3, CategoricalOrdering::Physical);
        dict_column.append_value("alpha");
        dict_column.append_value("beta");
        dict_column.append_null();
        dict_column.register_value("gamma");
        let dict_column = dict_column.finish();
        let dict_column = dict_column
            .into_series()
            .into_frame()
            .iter_chunks(CompatLevel::newest(), false)
            .next()
            .unwrap();
        let dict_column = record_batch_to_compat(dict_column).unwrap();
        let buf = Cursor::new(Vec::new());
        // let chunk = _into_record_batch(&dict_column).unwrap();
        let schema = Arc::new(dict_column.schema().clone());
        let fields = schema.iter().map(|x| x.1).cloned().collect::<Vec<_>>();
        let mut writer =
            FileWriter::try_new(buf, schema.clone(), None, Default::default()).unwrap();
        writer.write(&dict_column, None).unwrap();
        writer.write(&dict_column, None).unwrap();
        writer.finish().unwrap();

        let mut buf = writer.into_inner();
        buf.rewind().unwrap();
        let metadata = read_file_metadata(&mut buf).unwrap();

        let mut reader = FileReader::new(buf, metadata, None, None);
        let _ = get_next_df(&fields, &mut reader);
    }

    #[test]
    fn test_dictionary_df_roundtrip() {
        let mut dict_column =
            CategoricalChunkedBuilder::new("test".into(), 10, CategoricalOrdering::Physical);
        dict_column.append_value("alpha");
        dict_column.append_value("beta");
        dict_column.append_null();
        dict_column.register_value("gamma");
        let dict_column = dict_column.finish().into_series().into_frame();
        let buf = Cursor::new(Vec::new());
        let chunk = _into_record_batch(&dict_column).unwrap();
        let mut writer =
            FileWriter::try_new(buf, chunk.schema.clone(), None, Default::default()).unwrap();
        writer.write(&chunk.batch, None).unwrap();
        writer.finish().unwrap();

        let mut buf = writer.into_inner();
        buf.rewind().unwrap();
        let metadata = read_file_metadata(&mut buf).unwrap();

        let mut reader = FileReader::new(buf, metadata, None, None);
        let _ = reader.next().unwrap().unwrap();
    }

    // #[test]
    // fn test_dictionary_roundtrip() {
    //     let keys = PrimitiveArray::from_iter([0, 1, 2].into_iter().map(Some));
    //     let values = Utf8Array::<i32>::from([Some("hello"), Some("world"),
    // Some("thanks")]).boxed();     let dict =
    // DictionaryArray::try_from_keys(keys, values).unwrap();

    //     let name: PlSmallStr = "test".into();
    //     let field = ArrowField::new(name.clone(), dict.dtype().clone(), true);
    //     let mut schema: Schema<_> = Default::default();
    //     schema.insert(name, field);
    //     let schema = Arc::new(schema);

    //     let buf = Cursor::new(Vec::new());
    //     let mut writer =
    //         FileWriter::try_new(buf, schema.clone(), None,
    // Default::default()).unwrap();     let chunk = RecordBatch::new(3,
    // schema.clone(), vec![dict.boxed()]);     writer.write(&chunk,
    // None).unwrap();     writer.finish().unwrap();

    //     let mut buf = writer.into_inner();
    //     buf.rewind().unwrap();

    //     let metadata = read_file_metadata(&mut buf).unwrap();
    //     let mut reader = FileReader::new(buf, metadata, None, None);
    //     let _ = reader.next().unwrap().unwrap();
    // }

    #[test]
    fn test_signal_table_roundtrip() {
        let file = File::open("extra/multi_fast5_zip_v3.pod5").unwrap();
        let mut reader = Reader::from_reader(file).unwrap();
        let buf = Cursor::new(Vec::new());
        let mut reads = reader.signal_dfs().unwrap();
        let read_df = reads
            .next()
            .unwrap()
            .unwrap()
            .0
            .iter_chunks(CompatLevel::newest(), false)
            .next()
            .unwrap();
        let read_df = record_batch_to_compat(read_df).unwrap();
        // let chunk = read_df.into_record_batch().unwrap();
        let schema = Arc::new(read_df.schema().clone());
        // let fields = schema.iter().map(|x| x.1).cloned().collect::<Vec<_>>();

        let mut writer = FileWriter::try_new(buf, schema, None, Default::default()).unwrap();
        writer.write(&read_df, None).unwrap();
        writer.write(&read_df, None).unwrap();
        writer.finish().unwrap();

        let mut buf = writer.into_inner();
        buf.rewind().unwrap();
        let metadata = read_file_metadata(&mut buf).unwrap();
        // assert_eq!(
        //     metadata.ipc_schema,
        //     reads.table_reader.metadata().ipc_schema
        // );

        let fields = metadata
            .schema
            .iter()
            .map(|x| x.1.clone())
            .collect::<Vec<_>>();
        let mut reader = FileReader::new(buf, metadata, None, None);
        // let res = reader.next().unwrap().unwrap();
        let res = get_next_df(&fields, &mut reader);
        println!("{res:?}");
    }

    #[test]
    fn test_read_table_roundtrip2() {
        let file = File::open("extra/multi_fast5_zip_v3.pod5").unwrap();
        let mut reader = Reader::from_reader(file).unwrap();
        let buf = Cursor::new(Vec::new());
        let mut reads = reader.read_dfs().unwrap();
        let read_df = reads
            .next()
            .unwrap()
            .unwrap()
            .0
            .iter_chunks(CompatLevel::newest(), false)
            .next()
            .unwrap();
        // let chunk = read_df.into_record_batch().unwrap();

        let mut writer = FileWriter::try_new(
            buf,
            Arc::new(read_df.schema().clone()),
            None,
            Default::default(),
        )
        .unwrap();
        writer.write(&read_df, None).unwrap();
        writer.write(&read_df, None).unwrap();
        writer.finish().unwrap();

        let mut buf = writer.into_inner();
        buf.rewind().unwrap();
        let metadata = read_file_metadata(&mut buf).unwrap();
        assert_eq!(
            metadata.ipc_schema,
            reads.table_reader.metadata().ipc_schema
        );

        let mut reader = FileReader::new(buf, metadata, None, None);
        let _ = reader.next().unwrap().unwrap();
    }

    // PANICS: Solved by switching to iter_chunks
    // #[test]
    // fn test_read_table_roundtrip() {
    //     let file = File::open("extra/multi_fast5_zip_v3.pod5").unwrap();
    //     let mut reader = Reader::from_reader(file).unwrap();
    //     let buf = Cursor::new(Vec::new());
    //     let mut reads = reader.read_dfs().unwrap();
    //     let read_df = reads.next().unwrap().unwrap();
    //     let chunk = read_df.into_record_batch().unwrap();

    //     let mut writer =
    //         FileWriter::try_new(buf, chunk.schema.clone(), None,
    // Default::default()).unwrap();     writer.write(&chunk.batch,
    // None).unwrap();     writer.finish().unwrap();

    //     let mut buf = writer.into_inner();
    //     buf.rewind().unwrap();
    //     let metadata = read_file_metadata(&mut buf).unwrap();
    //     assert_eq!(
    //         metadata.ipc_schema,
    //         reads.table_reader.metadata().ipc_schema
    //     );

    //     let mut reader = FileReader::new(buf, metadata, None, None);
    //     let _ = reader.next().unwrap().unwrap();
    // }

    // #[test]
    // fn test_footer_roundtrip() {
    //     let inner = Cursor::new(Vec::<u8>::new());
    //     let mut writer = Writer::new(inner);
    //     writer._finish().unwrap();
    //     let inner = writer.into_inner();
    //     let binding = ParsedFooter::read_footer(inner).unwrap();
    //     let footer = binding.footer().unwrap();
    //     println!("{footer:?}");
    // }

    #[test]
    fn test_writer_reader_roundtrip() {
        let file = File::open("extra/multi_fast5_zip_v3.pod5").unwrap();
        let mut reader = Reader::from_reader(file).unwrap();
        println!("BEFORE: {:?}", reader.footer.footer());
        let buf = Cursor::new(Vec::new());
        let mut writer = Writer::from_writer(buf).unwrap();

        let mut run_info = reader.run_info_dfs().unwrap();
        let run_info_df = run_info.next().unwrap().unwrap();
        writer.write_table(&run_info_df).unwrap();

        let mut signals = reader.signal_dfs().unwrap();
        let signal_df = signals.next().unwrap().unwrap();
        writer.write_dataframe(&signal_df).unwrap();
        // writer
        //     .write_tables_with(|guard| {
        //         // guard.write_table(signal_df.clone())?;
        //         guard.write_table(signal_df.clone())?;
        //         Ok(())
        //     })
        //     .unwrap();

        let mut reads = reader.read_dfs().unwrap();
        let read_df = reads.next().unwrap().unwrap();
        // writer.write_table(&read_df).unwrap();
        // writer.write_tables_with(|guard| guard.write_table(&read_df)).unwrap();
        let guard = writer.write_and_guard(&read_df).unwrap();
        guard.finish2().unwrap();
        // writer.write_dataframe(&read_df).unwrap();

        writer._finish().unwrap();
        let mut inner = writer.into_inner();
        inner.rewind().unwrap();

        let mut reader = Reader::from_reader(inner).unwrap();
        println!("AFTER: {:?}", reader.footer.footer());

        let mut signals_rt = reader.signal_dfs().unwrap();
        let signal_df_rt = signals_rt.next().unwrap().unwrap();
        assert_eq!(signal_df, signal_df_rt);
        println!("Signal complete");

        let mut run_info_rt = reader.run_info_dfs().unwrap();
        let run_info_df_rt = run_info_rt.next().unwrap().unwrap();
        assert_eq!(run_info_df, run_info_df_rt);
        println!("Run info complete");

        let mut reads_rt = reader.read_dfs().unwrap();
        let read_df_rt = reads_rt.next().unwrap().unwrap();
        println!("read complete");
    }

    #[test]
    fn test_writer_signal_df() {
        let minknow_uuid = [
            "67e55044-10b1-426f-9247-bb680e5fe0c8",
            "67e55044-10b1-426f-9247-bb680e5fe0c8",
        ];
        let minknow_vbz = [
            [100i16, 200i16].iter().collect::<Series>(),
            [300i16, 400i16].iter().collect::<Series>(),
        ];
        let samples = [2u32, 2u32];
        let df = df!(
            "minknow.uuid" => minknow_uuid,
            "minknow.vbz" => minknow_vbz,
            "samples" => samples,
        )
        .unwrap();
        let df = SignalDataFrame(df);

        let buf = Cursor::new(Vec::new());
        let mut writer = Writer::from_writer(buf).unwrap();
        writer.write_table(&df).unwrap();
        writer.finish().unwrap();
    }
}
