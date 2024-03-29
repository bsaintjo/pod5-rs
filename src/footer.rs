use std::io::{Read, Seek, SeekFrom};

use flatbuffers::root;

use crate::{
    error::Pod5Error,
    footer_generated::minknow::reads_format::{ContentType, Footer},
    FILE_SIGNATURE,
};

#[derive(Debug)]
pub(crate) struct Table {
    offset: usize,
    length: usize,
}

impl Table {
    pub(crate) fn offset(&self) -> usize {
        self.offset
    }

    pub(crate) fn length(&self) -> usize {
        self.length
    }
}

#[derive(Debug)]
pub struct RunInfoTable(Table);

#[derive(Debug)]
pub(crate) struct ReadTable(Table);

impl AsRef<Table> for ReadTable {
    fn as_ref(&self) -> &Table {
        &self.0
    }
}

#[derive(Debug)]
pub(crate) struct SignalTable(Table);

impl AsRef<Table> for SignalTable {
    fn as_ref(&self) -> &Table {
        &self.0
    }
}

pub struct ParsedFooter {
    data: Vec<u8>,
}

impl ParsedFooter {
    pub fn footer(&self) -> Result<Footer<'_>, Pod5Error> {
        Ok(root::<Footer>(&self.data)?)
    }

    fn find_table(&self, content_type: ContentType, err: Pod5Error) -> Result<Table, Pod5Error> {
        let footer = self.footer()?;
        let contents = footer.contents().ok_or(Pod5Error::ContentsMissing)?;
        let mut efile = None;
        for c in contents {
            if c.content_type() == content_type {
                efile = Some(c);
                break;
            }
        }
        let efile = efile.ok_or(err)?;

        Ok(Table {
            offset: efile.offset() as usize,
            length: efile.length() as usize,
        })
    }

    pub(crate) fn read_table(&self) -> Result<ReadTable, Pod5Error> {
        Ok(ReadTable(self.find_table(
            ContentType::ReadsTable,
            Pod5Error::ReadTableMissing,
        )?))
    }

    pub(crate) fn signal_table(&self) -> Result<SignalTable, Pod5Error> {
        Ok(SignalTable(self.find_table(
            ContentType::SignalTable,
            Pod5Error::SignalTableMissing,
        )?))
    }

    pub(crate) fn read_footer<R: Read + Seek>(mut reader: R) -> Result<Self, Pod5Error> {
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

    pub(crate) fn run_info_table(&self) -> Result<RunInfoTable, Pod5Error> {
        Ok(RunInfoTable(self.find_table(
            ContentType::RunInfoTable,
            Pod5Error::RunInfoTable,
        )?))
    }
}

#[cfg(test)]
mod test {

    use std::fs::File;

    use super::*;

    #[test]
    fn test_footer() -> eyre::Result<()> {
        let path = "extra/multi_fast5_zip_v3.pod5";
        let file = File::open(path)?;
        let footer = ParsedFooter::read_footer(&file)?;
        let read_table = footer.read_table()?;
        // let reader = read_embedded_arrow(&file, &read_table.0)?;
        // println!("{:?}\n", reader.schema());

        // let chunk = reader.next().unwrap();
        // let chunk = chunk?;
        // let arr = chunk.arrays()[1].as_ref();
        // println!("{arr:?}");
        Ok(())
    }
}
