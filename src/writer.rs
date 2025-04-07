//! Writing POD5 files
//!
//! Provides an interface for writing dataframes as POD5 tables.
use std::{
    collections::HashSet,
    io::{Seek, Write},
    marker::PhantomData,
    sync::Arc,
};

use polars::{error::PolarsError, prelude::ArrowField};
use polars_arrow::{
    io::ipc::write::{FileWriter, WriteOptions as PlWriteOptions},
    record_batch::RecordBatch,
};
use polars_schema::Schema;
use uuid::Uuid;

use crate::{
    dataframe::{
        compatibility::{field_arrs_to_record_batch, field_arrs_to_schema, series_to_array},
        ReadDataFrame, RunInfoDataFrame, SignalDataFrame,
    },
    footer::footer_generated::minknow::reads_format::{
        ContentType, EmbeddedFile, EmbeddedFileArgs, Footer, FooterArgs,
    },
    FILE_SIGNATURE,
};

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
}

pub struct TableBatch {
    batch: RecordBatch,
    schema: Arc<Schema<ArrowField>>,
}

pub trait IntoTable {
    fn into_record_batch(self) -> Result<TableBatch, PolarsError>;
    fn content_type() -> ContentType {
        ContentType::OtherIndex
    }
}

impl IntoTable for SignalDataFrame {
    fn into_record_batch(self) -> Result<TableBatch, PolarsError> {
        _into_record_batch(&self.0)
    }

    fn content_type() -> ContentType {
        ContentType::SignalTable
    }
}
impl IntoTable for ReadDataFrame {
    fn into_record_batch(self) -> Result<TableBatch, PolarsError> {
        _into_record_batch(&self.0)
    }

    fn content_type() -> ContentType {
        ContentType::ReadsTable
    }
}
impl IntoTable for RunInfoDataFrame {
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
    tables: Vec<TableInfo>,
    contents_writtens: HashSet<ContentType>,
    footer_written: bool,
}

impl<W: Write + Seek> Writer<W> {
    pub(crate) fn new(writer: W) -> Self {
        let section_marker = Uuid::new_v4();
        Self {
            position: 0,
            writer,
            section_marker,
            tables: Vec::new(),
            contents_writtens: HashSet::new(),
            footer_written: false,
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

    /// Write a table to the POD5 file using a write guard. At the end of the closure,
    /// the section marker is written to the file, and the Writer is updated with
    /// the written content type.
    ///
    /// This is primarily useful for writing multiple tables correctly. If you only need
    /// to write a single table, use the `write_table` method.
    ///
    /// This method will return an error if the content type is not `OtherIndex` and
    /// the content type has already been written. This is to prevent writing multiple
    /// tables of the same type to the file.
    pub fn write_tables_with<F, T>(&mut self, inserter: F) -> Result<(), WriteError>
    where
        F: FnOnce(&mut TableWriteGuard<W, T>) -> Result<(), WriteError>,
        T: IntoTable,
    {
        let new_content = T::content_type();
        if new_content != ContentType::OtherIndex && self.contents_writtens.contains(&new_content) {
            return Err(WriteError::ContentTypeAlreadyWritten(new_content));
        }
        inserter(&mut TableWriteGuard::new(self))?;
        Ok(())
    }

    /// Write a single table to the POD5 file. The writer is updated with
    ///
    /// This is a convienence function for writing a single table to the file.
    /// If you need to write multiple tables, use the `write_tables_with` method.
    ///
    /// Similar to writing with the `write_tables_with` method, this will return an
    /// error if the content type is not `OtherIndex` and the content type has
    /// already been written.
    pub fn write_table<D: IntoTable>(&mut self, df: D) -> Result<(), WriteError> {
        self.write_tables_with(|guard| guard.write_table(df))?;
        Ok(())
    }

    pub fn write_table_iter<I, D>(&mut self, iter: I) -> Result<(), WriteError>
    where
        I: Iterator<Item = D>,
        D: IntoTable,
    {
        self.write_tables_with(|guard| {
            for df in iter {
                guard.write_table(df)?;
            }
            Ok(())
        })?;
        Ok(())
    }

    /// Write the flatbuffers footer and last signature bits to finish writing the file.
    pub fn finish(mut self) -> Result<(), WriteError> {
        self._finish()?;
        Ok(())
    }

    pub(crate) fn _finish(&mut self) -> Result<(), WriteError> {
        self.write_footer_magic()?;
        self.write_footer()?;
        Ok(())
    }

    fn write_footer_magic(&mut self) -> Result<(), WriteError> {
        self.writer
            .write_all(&FOOTER_MAGIC)
            .map_err(WriteError::FailedToWriteFooterMagic)?;
        Ok(())
    }

    fn write_footer(&mut self) -> Result<(), WriteError> {
        let mut footer = build_footer2(&self.tables);
        let padding_len = footer.len() % 8;
        footer.extend_from_slice(&vec![0; padding_len]);
        self.writer
            .write_all(&footer)
            .map_err(WriteError::FailedToWriteFooter)?;
        Ok(())
    }
}

const FOOTER_MAGIC: [u8; 8] = [b'F', b'O', b'O', b'T', b'E', b'R', 0x000, 0x000];

fn build_footer2(table_infos: &[TableInfo]) -> Vec<u8> {
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

    let file_identifier = Some(builder.create_string("some file identifier"));
    let software = Some(builder.create_string("some software"));
    let pod5_version = Some(builder.create_string("0.3.0"));

    let fbtable = Footer::create(
        &mut builder,
        &FooterArgs {
            file_identifier,
            software,
            pod5_version,
            contents,
        },
    );

    builder.finish(fbtable, None);
    builder.finished_data().to_vec()
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
    inner: &'a mut Writer<W>,
    table: PhantomData<T>,
}

impl<'a, W, T> TableWriteGuard<'a, W, T>
where
    W: Write + Seek,
    T: IntoTable,
{
    fn new(inner: &'a mut Writer<W>) -> Self {
        Self {
            inner,
            table: PhantomData,
        }
    }

    pub fn write_table(&mut self, df: T) -> Result<(), WriteError> {
        let chunk = df.into_record_batch()?;
        let mut writer = FileWriter::try_new(
            &mut self.inner.writer,
            chunk.schema.clone(),
            None,
            PlWriteOptions::default(),
        )?;
        writer.write(&chunk.batch, None)?;
        Ok(())
    }
}

impl<W, T> Drop for TableWriteGuard<'_, W, T>
where
    W: Write + Seek,
    T: IntoTable,
{
    fn drop(&mut self) {
        self.inner.write_section_marker().unwrap();
        let new_position = self
            .inner
            .writer
            .stream_position()
            .map_err(WriteError::StreamPositionError)
            .unwrap();
        let offset = self.inner.position as i64;
        let length = new_position as i64 - self.inner.position as i64;
        self.inner.tables.push(TableInfo {
            offset,
            length,
            content_type: T::content_type(),
        });
    }
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::Cursor};

    use polars::{df, series::Series};

    use crate::reader::Reader;

    use super::*;

    #[test]
    fn test_writer_reader_roundtrip() {
        let file = File::open("extra/multi_fast5_zip_v3.pod5").unwrap();
        let mut reader = Reader::from_reader(file).unwrap();
        let buf = Cursor::new(Vec::new());
        let mut writer = Writer::from_writer(buf).unwrap();

        let mut reads = reader.read_dfs().unwrap();
        let read_df = reads.next().unwrap().unwrap();
        writer.write_table(read_df).unwrap();

        let mut signals = reader.signal_dfs().unwrap();
        let signal_df = signals.next().unwrap().unwrap();
        writer.write_table(signal_df).unwrap();

        let mut run_info = reader.run_info_dfs().unwrap();
        let run_info_df = run_info.next().unwrap().unwrap();
        writer.write_table(run_info_df).unwrap();

        writer.finish().unwrap();
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
        writer.write_table(df).unwrap();
        writer.finish().unwrap();
    }
}
