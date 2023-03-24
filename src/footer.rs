use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use flatbuffers::root;

use crate::{
    error::Pod5Error,
    footer_generated::minknow::reads_format::{ContentType, Footer},
    FILE_SIGNATURE,
};

#[derive(Debug)]
struct Table {
    offset: usize,
    length: usize
}

#[derive(Debug)]
pub struct ReadTable(Table);

#[derive(Debug)]
pub struct SignalTable(Table);

pub struct ParsedFooter {
    data: Vec<u8>,
}

impl ParsedFooter {
    pub fn footer(&self) -> Result<Footer<'_>, Pod5Error> {
        Ok(root::<Footer>(&self.data)?)
    }

    fn find_table(&self, content_type: ContentType) -> Result<Option<Table>, Pod5Error> {
        let footer = self.footer()?;
        let contents = footer.contents().ok_or(Pod5Error::ContentsMissing)?;
        let mut efile = None;
        for c in contents {
            if c.content_type() == content_type {
                efile = Some(c);
                break;
            }
        }
        Ok(efile.map(|e| Table { offset: e.offset() as usize, length: e.length() as usize }))
    }

    pub fn read_table(&self) -> Result<Option<ReadTable>, Pod5Error> {
        Ok(self
            .find_table(ContentType::ReadsTable)?
            .map(ReadTable))
    }

    pub fn signal_table(&self) -> Result<Option<SignalTable>, Pod5Error> {
        Ok(self.find_table(ContentType::SignalTable)?.map(SignalTable))
    }

    pub fn read_footer(mut file: &File) -> Result<Self, Pod5Error> {
        file.rewind()?;
        let file_size = file.metadata()?.len();
        let footer_length_end: u64 = (file_size - FILE_SIGNATURE.len() as u64) - 16;
        let footer_length = footer_length_end - 8;
        file.seek(SeekFrom::Start(footer_length))?;
        let mut buf = [0; 8];
        file.read_exact(&mut buf)?;
        let flen = i64::from_le_bytes(buf);
        file.seek(SeekFrom::Start(footer_length - (flen as u64)))?;
        let mut buf = vec![0u8; flen as usize];
        file.read_exact(&mut buf)?;
        Ok(Self { data: buf })
    }
}

#[cfg(test)]
mod test {

    use crate::reader::read_embedded_arrow;

    use super::*;

    #[test]
    fn test_footer() -> eyre::Result<()> {
        let path = "extra/multi_fast5_zip_v3.pod5";
        let file = File::open(path)?;
        let footer = ParsedFooter::read_footer(&file)?;
        let read_table = footer.read_table()?.unwrap();
        // let reader = read_embedded_arrow(&file, &read_table.0)?;
        // println!("{:?}\n", reader.schema());

        // let chunk = reader.next().unwrap();
        // let chunk = chunk?;
        // let arr = chunk.arrays()[1].as_ref();
        // println!("{arr:?}");
        Ok(())
    }
}
