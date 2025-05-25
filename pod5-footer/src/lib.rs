use std::io::{self, Read, Seek, SeekFrom};

use flatbuffers::{InvalidFlatbuffer, root};
use footer_generated::minknow::reads_format::{EmbeddedFile, EmbeddedFileArgs, FooterArgs};

use crate::footer_generated::minknow::reads_format::{ContentType, Footer};

pub mod footer_generated;

const FILE_SIGNATURE: [u8; 8] = [0x8b, b'P', b'O', b'D', b'\r', b'\n', 0x1a, b'\n'];
pub const FOOTER_MAGIC: [u8; 8] = [b'F', b'O', b'O', b'T', b'E', b'R', 0x000, 0x000];

#[derive(thiserror::Error, Debug)]
pub enum FooterError {
    #[error("FlatBuffers error: {0}")]
    FlatBuffersError(#[from] InvalidFlatbuffer),

    #[error("Footer IO Error: {0}")]
    FooterIOError(#[from] std::io::Error),

    #[error(
        "Missing list of embedded files from footer, footer is likely improperly constructed or pod5 is empty"
    )]
    ContentsMissing,

    #[error("Missing Signal table from POD5")]
    SignalTableMissing,

    #[error("Missing Read table from POD5")]
    ReadTableMissing,

    #[error("Missing Run Info table from POD5")]
    RunInfoTableMissing,
}

#[derive(Debug)]
pub struct TableInfo {
    offset: i64,
    length: i64,
    content_type: ContentType,
}

impl TableInfo {
    pub fn new(offset: i64, length: i64, content_type: ContentType) -> Self {
        Self { offset, length, content_type }
    }
    
    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn length(&self) -> i64 {
        self.length
    }
}

#[derive(Debug)]
pub struct RunInfoTable(TableInfo);

impl RunInfoTable {
    pub fn as_ref(&self) -> &TableInfo {
        &self.0
    }
}

#[derive(Debug)]
pub struct ReadTable(TableInfo);
impl ReadTable {
    pub fn as_ref(&self) -> &TableInfo {
        &self.0
    }

    pub fn read_to_buf<R: Read + Seek>(
        &self,
        reader: &mut R,
        buf: &mut [u8],
    ) -> Result<(), io::Error> {
        let offset = self.0.offset() as u64;
        let length = self.0.length() as u64;

        reader.seek(SeekFrom::Start(offset))?;
        reader.read_exact(buf)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SignalTable(TableInfo);

impl SignalTable {
    pub fn read_to_buf<R: Read + Seek>(
        &self,
        reader: &mut R,
        buf: &mut [u8],
    ) -> Result<(), io::Error> {
        let offset = self.0.offset() as u64;
        let length = self.0.length() as u64;

        reader.seek(SeekFrom::Start(offset))?;
        reader.read_exact(buf)?;
        Ok(())
    }
}

impl AsRef<TableInfo> for SignalTable {
    fn as_ref(&self) -> &TableInfo {
        &self.0
    }
}

pub struct ParsedFooter {
    data: Vec<u8>,
}

impl ParsedFooter {
    pub fn footer(&self) -> Result<Footer<'_>, FooterError> {
        Ok(root::<Footer>(&self.data)?)
    }

    fn find_table(
        &self,
        content_type: ContentType,
        err: FooterError,
    ) -> Result<TableInfo, FooterError> {
        let footer = self.footer()?;
        let contents = footer.contents().ok_or(FooterError::ContentsMissing)?;
        let mut efile = None;
        for c in contents {
            if c.content_type() == content_type {
                efile = Some(c);
                break;
            }
        }
        let efile = efile.ok_or(err)?;

        Ok(TableInfo {
            offset: efile.offset(),
            length: efile.length(),
            content_type: content_type,
        })
    }

    pub fn read_table(&self) -> Result<ReadTable, FooterError> {
        Ok(ReadTable(self.find_table(
            ContentType::ReadsTable,
            FooterError::ReadTableMissing,
        )?))
    }

    pub fn signal_table(&self) -> Result<SignalTable, FooterError> {
        Ok(SignalTable(self.find_table(
            ContentType::SignalTable,
            FooterError::SignalTableMissing,
        )?))
    }

    pub fn run_info_table(&self) -> Result<RunInfoTable, FooterError> {
        Ok(RunInfoTable(self.find_table(
            ContentType::RunInfoTable,
            FooterError::RunInfoTableMissing,
        )?))
    }

    pub fn read_footer<R: Read + Seek>(mut reader: R) -> Result<Self, FooterError> {
        reader.rewind()?;
        // let file_size = reader.stream_len()?;
        // let footer_length_end: u64 = (file_size - FILE_SIGNATURE.len() as u64) - 16;
        // let footer_length = footer_length_end - 8;
        let footer_length = -(FILE_SIGNATURE.len() as i64) + (-16) + (-8);
        reader.seek(SeekFrom::End(footer_length))?;
        let mut buf = [0; 8];
        reader.read_exact(&mut buf)?;
        let flen = i64::from_le_bytes(buf);
        reader.seek(SeekFrom::End(footer_length - flen))?;
        let mut buf = vec![0u8; flen as usize];
        reader.read_exact(&mut buf)?;
        Ok(Self { data: buf })
    }
}

pub struct FooterBuilder {
    file_identifier: String,
    software: String,
    version: String,
}

impl FooterBuilder {
    pub fn new(file_identifier: String, software: String, version: String) -> Self {
        Self {
            file_identifier,
            software,
            version,
        }
    }

    pub fn build_footer(&self, tables: &[TableInfo]) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let mut etables = Vec::with_capacity(tables.len());
        for table in tables {
            let efile_args = EmbeddedFileArgs {
                offset: table.offset as i64,
                length: table.length as i64,
                content_type: table.content_type,
                ..Default::default()
            };
            let efile = EmbeddedFile::create(&mut builder, &efile_args);
            etables.push(efile);
        }
        let contents = Some(builder.create_vector(&etables));

        let file_identifier = Some(builder.create_string(&self.file_identifier));
        let software = Some(builder.create_string(&self.software));
        let pod5_version = Some(builder.create_string(&self.version));

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
}

#[cfg(test)]
mod test {

    use std::fs::File;

    use super::*;

    #[test]
    fn test_footer() -> eyre::Result<()> {
        let path = "../extra/multi_fast5_zip_v3.pod5";
        let file = File::open(path)?;
        let footer = ParsedFooter::read_footer(&file)?;
        assert!(footer.read_table().is_ok());
        assert!(footer.run_info_table().is_ok());
        assert!(footer.signal_table().is_ok());
        Ok(())
    }
}
