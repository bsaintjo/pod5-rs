//! Writing POD5 files
//!
//! Provides an interface for writing dataframes as POD5 tables.
use std::{
    collections::{BTreeMap, HashSet},
    io::{Seek, Write},
    marker::PhantomData,
    sync::Arc,
};

use pod5_footer::{FOOTER_MAGIC, TableInfo, footer_generated::minknow::reads_format::ContentType};
use polars::{
    error::PolarsError,
    frame::DataFrame,
    prelude::{CompatLevel, PlSmallStr},
};
use polars_arrow::{datatypes::Metadata, io::ipc::write::FileWriter};
use uuid::Uuid;

use crate::{
    FILE_SIGNATURE,
    dataframe::{
        ReadDataFrame, RunInfoDataFrame, SignalDataFrame,
        compatibility::record_batch_to_compat,
        schema::{
            TableSchema, reads_schema::ReadSchema, run_info_schema::RunInfoSchema,
            signal_schema::SignalSchema,
        },
    },
};

const SOFTWARE: &str = "pod5-rs";
const POD5_VERSION: &str = "0.0.40";

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

#[derive(Debug, Clone)]
pub enum TableContent {
    Signal,
    Read,
    RunInfo,
    Other,
}

impl TableContent {
    fn into_content_type(self) -> ContentType {
        match self {
            TableContent::Signal => ContentType::SignalTable,
            TableContent::Read => ContentType::ReadsTable,
            TableContent::RunInfo => ContentType::RunInfoTable,
            TableContent::Other => ContentType::OtherIndex,
        }
    }
}

pub trait IntoTable {
    type Schema: TableSchema;
    fn as_dataframe(&self) -> &DataFrame;
    fn content_type() -> TableContent;
}

impl IntoTable for SignalDataFrame {
    type Schema = SignalSchema;
    fn as_dataframe(&self) -> &DataFrame {
        &self.0
    }

    fn content_type() -> TableContent {
        TableContent::Signal
    }
}

impl IntoTable for ReadDataFrame {
    type Schema = ReadSchema;
    fn as_dataframe(&self) -> &DataFrame {
        &self.0
    }

    fn content_type() -> TableContent {
        TableContent::Read
    }
}
impl IntoTable for RunInfoDataFrame {
    type Schema = RunInfoSchema;
    fn as_dataframe(&self) -> &DataFrame {
        &self.0
    }

    fn content_type() -> TableContent {
        TableContent::RunInfo
    }
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
    // footer_written: bool,
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
            // footer_written: false,
            file_identifier,
            metadata,
        }
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

    fn end_table(&mut self, content_type: ContentType) -> Result<(), WriteError> {
        let new_position = self
            .writer
            .stream_position()
            .map_err(WriteError::StreamPositionError)?;
        // Padding the footer to 8-byte boundary
        let padding = 8 - (new_position % 8);
        self.write_all(&vec![0u8; padding as usize]).unwrap();
        self.write_section_marker().unwrap();
        let offset = self.position as i64;
        let length = new_position as i64 - self.position as i64;
        self.tables
            .push(TableInfo::new(offset, length, content_type));
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
        let footer = self.build_footer();
        self.writer
            .write_all(&footer)
            .map_err(WriteError::FailedToWriteFooter)?;

        let footer_len_bytes = (footer.len() as i64).to_le_bytes();
        self.writer
            .write_all(&footer_len_bytes)
            .map_err(|_| WriteError::FailedToWriteFooterLengthBytes)?;

        Ok(footer.len() as u64)
    }

    pub fn guard<T: IntoTable>(&mut self) -> TableWriteGuard<'_, W, T> {
        let metadata = self.metadata.clone();
        TableWriteGuard {
            inner: Some(TableWriter::PreInit(self)),
            metadata,
            table: PhantomData,
        }
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
    ///
    /// This method will fail if a table of the same type has already been
    /// written ```
    /// # fn main() -> Result<(), WriteError>{
    /// # let file = File::open("extra/multi_fast5_zip_v3.pod5").unwrap();
    /// # let mut reader = Reader::from_reader(file).unwrap();
    /// # let buf = Cursor::new(Vec::new());
    /// # let mut writer = Writer::from_writer(buf).unwrap();
    /// # let mut run_info = reader.run_info_dfs().unwrap();
    /// # let run_info_df = run_info.next().unwrap().unwrap();
    /// writer.with_guard::<RunInfoDataFrame, _>(|g|
    /// g.write_batch(&run_info_df))?;
    ///
    /// /// Subsequent attempts to write run info tables will fail
    /// assert!(writer.with_guard::<RunInfoDataFrame, _>(|g|
    /// g.write_batch(&run_info_df)).is_err()); #}
    /// ```
    pub fn with_guard<T, F>(&mut self, mut closure: F) -> Result<(), WriteError>
    where
        T: IntoTable,
        F: FnMut(&mut TableWriteGuard<W, T>) -> Result<(), WriteError>,
    {
        if T::content_type().into_content_type() != ContentType::OtherIndex
            && self
                .contents_writtens
                .contains(&T::content_type().into_content_type())
        {
            return Err(WriteError::ContentTypeAlreadyWritten(
                T::content_type().into_content_type(),
            ));
        }
        let mut guard = self.guard::<T>();
        closure(&mut guard)?;
        guard.finish()?;
        Ok(())
    }

    fn build_footer(&self) -> Vec<u8> {
        pod5_footer::FooterBuilder::new(
            self.file_identifier.to_string(),
            SOFTWARE.to_string(),
            POD5_VERSION.to_string(),
        )
        .build_footer(&self.tables)
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

fn pod5_metadata<S: Into<PlSmallStr>>(file_identifier: S) -> Arc<BTreeMap<PlSmallStr, PlSmallStr>> {
    let mut metadata = Metadata::new();
    metadata.insert("MINKNOW:pod5_version".into(), POD5_VERSION.into());
    metadata.insert("MINKNOW:software".into(), SOFTWARE.into());
    metadata.insert("MINKNOW:file_identifier".into(), file_identifier.into());
    Arc::new(metadata)
}

impl<'a, W, T> TableWriteGuard<'a, W, T>
where
    W: Write + Seek,
    T: IntoTable,
{
    pub fn new(writer: &'a mut Writer<W>) -> Result<Self, WriteError> {
        let metadata = pod5_metadata(writer.file_identifier.to_string());
        let mut writer = FileWriter::new(writer, T::Schema::as_schema(), None, Default::default());
        writer.set_custom_schema_metadata(metadata.clone());
        writer.start()?;
        Ok(TableWriteGuard {
            inner: Some(TableWriter::PostInit(writer)),
            metadata: metadata,
            table: PhantomData,
        })
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

    // pub fn write_table(&mut self, df: &T) -> Result<(), WriteError> {
    //     let batches = df
    //         .as_dataframe()
    //         .iter_chunks(CompatLevel::newest(), false)
    //         .collect::<Vec<_>>();
    //     let schema = Arc::new(batches[0].schema().clone());

    //     let mut w = match self.inner.take() {
    //         Some(TableWriter::PreInit(writer)) => {
    //             let mut writer = FileWriter::new(writer, schema, None,
    // Default::default());
    // writer.set_custom_schema_metadata(self.metadata.clone());
    // writer.start()?;             writer
    //         }
    //         Some(TableWriter::PostInit(writer)) => writer,
    //         None => {
    //             panic!("Shouldn't be able to get here")
    //         }
    //     };
    //     for chunk in batches.into_iter() {
    //         let chunk = record_batch_to_compat(chunk).unwrap();
    //         w.write(&chunk, None)?;
    //     }
    //     self.inner = Some(TableWriter::PostInit(w));
    //     Ok(())
    // }

    pub fn write_batch(&mut self, df: &T) -> Result<(), WriteError> {
        let batch = df
            .as_dataframe()
            .iter_chunks(CompatLevel::newest(), false)
            .next()
            .unwrap();
        let batch = record_batch_to_compat(batch).unwrap();
        let schema = Arc::new(batch.schema().clone());

        let mut w = match self.inner.take() {
            Some(TableWriter::PreInit(writer)) => {
                let mut writer = FileWriter::new(writer, schema, None, Default::default());
                writer.set_custom_schema_metadata(self.metadata.clone());
                writer.start()?;
                writer
            }
            Some(TableWriter::PostInit(writer)) => writer,
            None => {
                panic!("Writer missing; should not be possible")
            }
        };

        w.write(&batch, None).unwrap();
        self.inner = Some(TableWriter::PostInit(w));
        Ok(())
    }

    pub fn finish(mut self) -> Result<(), WriteError> {
        if let Some(TableWriter::PostInit(mut x)) = self.inner.take() {
            x.finish()?;
            let inner = x.into_inner();
            inner.end_table(T::content_type().into_content_type())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::Cursor};

    use polars::{
        df,
        prelude::{CategoricalChunkedBuilder, CategoricalOrdering, CompatLevel},
        series::{IntoSeries, Series},
    };
    use polars_arrow::io::ipc::read::{FileReader, read_file_metadata};

    use super::*;
    use crate::{
        dataframe::{compatibility::record_batch_to_compat, get_next_df},
        reader::Reader,
    };

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
    fn test_signal_table_roundtrip() {
        let file = File::open("../extra/multi_fast5_zip_v3.pod5").unwrap();
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
        let schema = Arc::new(read_df.schema().clone());

        let mut writer = FileWriter::try_new(buf, schema, None, Default::default()).unwrap();
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

        let fields = metadata
            .schema
            .iter()
            .map(|x| x.1.clone())
            .collect::<Vec<_>>();
        let mut reader = FileReader::new(buf, metadata, None, None);
        let res = get_next_df(&fields, &mut reader);
        println!("{res:?}");
    }

    #[test]
    fn test_read_table_roundtrip2() {
        let file = File::open("../extra/multi_fast5_zip_v3.pod5").unwrap();
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
        let file = File::open("../extra/multi_fast5_zip_v3.pod5").unwrap();
        let mut reader = Reader::from_reader(file).unwrap();
        println!("BEFORE: {:?}", reader.footer.footer());
        let buf = Cursor::new(Vec::new());
        let mut writer = Writer::from_writer(buf).unwrap();

        let mut run_info = reader.run_info_dfs().unwrap();
        let run_info_df = run_info.next().unwrap().unwrap();
        // writer.write_table(&run_info_df).unwrap();
        writer.with_guard(|g| g.write_batch(&run_info_df)).unwrap();

        let mut signals = reader.signal_dfs().unwrap();
        let signal_df = signals.next().unwrap().unwrap();
        writer.with_guard(|g| g.write_batch(&signal_df)).unwrap();
        // writer.write_dataframe(&signal_df).unwrap();
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
        // let guard = writer.write_and_guard(&read_df).unwrap();
        // guard.finish2().unwrap();
        // let mut guard = writer.guard::<ReadDataFrame>();
        // guard.write_table2(&read_df).unwrap();
        // guard.finish2().unwrap();
        writer
            .with_guard::<ReadDataFrame, _>(|guard| {
                guard.write_batch(&read_df)?;
                guard.write_batch(&read_df)?;
                Ok(())
            })
            .unwrap();
        // writer.write_dataframe(&read_df).unwrap();

        writer._finish().unwrap();
        let mut inner = writer.writer;
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

        // We wrote two batches so this should not panic
        let _ = reads_rt.next().unwrap().unwrap();
        assert_eq!(read_df_rt, read_df);
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
            "read_id" => minknow_uuid,
            "signal" => minknow_vbz,
            "samples" => samples,
        )
        .unwrap();
        let df = SignalDataFrame(df);

        let buf = Cursor::new(Vec::new());
        let mut writer = Writer::from_writer(buf).unwrap();
        writer.with_guard(|g| g.write_batch(&df)).unwrap();
    }
}
