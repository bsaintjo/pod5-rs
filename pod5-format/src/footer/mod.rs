use std::io::{self, Read, Seek, SeekFrom, Write};

use flatbuffers::root;
use footer_generated::minknow::reads_format::{
    ContentType, EmbeddedFile, EmbeddedFileArgs, Footer, FooterArgs,
};

use crate::{FILE_SIGNATURE, FormatError, error::FooterError};

#[allow(warnings)] // Ignore warnings from generated file.
pub mod footer_generated;

pub const FOOTER_MAGIC: [u8; 8] = [b'F', b'O', b'O', b'T', b'E', b'R', 0x000, 0x000];

/// Contains information about the location, size, and type of a POD5 Table
#[derive(Debug)]
pub struct TableInfo {
    offset: i64,
    length: i64,
    content_type: ContentType,
}

impl TableInfo {
    pub fn new(offset: i64, length: i64, content_type: ContentType) -> Self {
        Self {
            offset,
            length,
            content_type,
        }
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
        // let length = self.0.length() as u64;

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
        // let length = self.0.length() as u64;

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
    /// Parse a POD5 Flatbuffer footer from a reader containg data from a POD5
    /// file.
    pub fn read_footer<R: Read + Seek>(mut reader: R) -> Result<Self, FormatError> {
        reader.rewind().map_err(FooterError::FooterIOError)?;
        // let file_size = reader.stream_len()?;
        // let footer_length_end: u64 = (file_size - FILE_SIGNATURE.len() as u64) - 16;
        // let footer_length = footer_length_end - 8;
        let footer_length = -(FILE_SIGNATURE.len() as i64) + (-16) + (-8);
        reader
            .seek(SeekFrom::End(footer_length))
            .map_err(FooterError::FooterIOError)?;
        let mut buf = [0; 8];
        reader
            .read_exact(&mut buf)
            .map_err(FooterError::FooterIOError)?;
        let flen = i64::from_le_bytes(buf);
        reader
            .seek(SeekFrom::End(footer_length - flen))
            .map_err(FooterError::FooterIOError)?;
        let mut buf = vec![0u8; flen as usize];
        reader
            .read_exact(&mut buf)
            .map_err(FooterError::FooterIOError)?;
        Ok(Self { data: buf })
    }

    pub fn footer(&self) -> Result<Footer<'_>, FooterError> {
        Ok(root::<Footer>(&self.data)?)
    }

    fn find_table(
        &self,
        content_type: ContentType,
        err: FooterError,
    ) -> Result<TableInfo, FormatError> {
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

    pub fn read_table(&self) -> Result<ReadTable, FormatError> {
        Ok(ReadTable(self.find_table(
            ContentType::ReadsTable,
            FooterError::ReadTableMissing,
        )?))
    }

    pub fn signal_table(&self) -> Result<SignalTable, FormatError> {
        Ok(SignalTable(self.find_table(
            ContentType::SignalTable,
            FooterError::SignalTableMissing,
        )?))
    }

    pub fn run_info_table(&self) -> Result<RunInfoTable, FormatError> {
        Ok(RunInfoTable(self.find_table(
            ContentType::RunInfoTable,
            FooterError::RunInfoTableMissing,
        )?))
    }
}

/// Build a new POD5 FlatBuffer's footer, useful for writing new POD5 files.
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

    /// Convert the builder and list of tables into the corresponding flatbuffer
    /// footer bytes.
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

    /// Write the FlatBuffers footer according to the [POD5 file specification](https://pod5-file-format.readthedocs.io/en/latest/SPECIFICATION.html#combined-file-layout)
    ///
    /// This method will write the:
    /// ```text
    /// <footer magic: "FOOTER\000\000">
    /// <footer (padded to 8-byte boundary)>
    /// <footer length: 8 bytes little-endian signed integer>
    /// ```
    /// sections to the writer.
    ///
    /// NOTE: I've tried to pad the footer to an 8-byte boundary according to
    /// the specification, however, I've run into issues with the padded
    /// footer being parsed by the official `pod5` tools. The flatbuffers
    /// library may already pad the write. For now, the current iteration is
    /// correctly parsed by the official `pod5` tools, in case you wonder
    /// why there isn't any code for padding in the source.
    pub fn write_footer<W>(&self, tables: &[TableInfo], writer: &mut W) -> Result<(), FormatError>
    where
        W: Write,
    {
        // Footer magic
        writer
            .write_all(&FOOTER_MAGIC)
            .map_err(FooterError::FooterIOError)?;

        // Footer
        let footer = self.build_footer(tables);
        writer
            .write_all(&footer)
            .map_err(FooterError::FooterIOError)?;

        let footer_len_bytes = (footer.len() as i64).to_le_bytes();
        writer
            .write_all(&footer_len_bytes)
            .map_err(FooterError::FooterIOError)?;

        Ok(())
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
