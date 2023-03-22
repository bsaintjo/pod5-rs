use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use flatbuffers::root;

use crate::{
    error::Pod5Error,
    footer_generated::minknow::reads_format::{ContentType, EmbeddedFile, Footer},
    FILE_SIGNATURE,
};

#[derive(Debug)]
struct EmbeddedReadTable<'a>(EmbeddedFile<'a>);

pub struct ParsedFooter {
    data: Vec<u8>,
}

impl ParsedFooter {
    fn footer(&self) -> Result<Footer<'_>, Pod5Error> {
        Ok(root::<Footer>(&self.data)?)
    }

    fn find_table(&self, content_type: ContentType) -> Result<Option<EmbeddedFile>, Pod5Error> {
        let footer = self.footer()?;
        let contents = footer.contents().ok_or(Pod5Error::ContentsMissing)?;
        let mut efile = None;
        for c in contents {
            if c.content_type() == content_type {
                efile = Some(c);
                break;
            }
        }
        Ok(efile)
    }

    fn read_table(&self) -> Result<Option<EmbeddedReadTable>, Pod5Error> {
        Ok(self
            .find_table(ContentType::ReadsTable)?
            .map(EmbeddedReadTable))
    }

    fn signal_table(&self) -> Result<Option<EmbeddedFile>, Pod5Error> {
        todo!()
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
        let reader = read_embedded_arrow(&file, &read_table.0)?;
        println!("{:?}\n", reader.schema());

        // let chunk = reader.next().unwrap();
        // let chunk = chunk?;
        // let arr = chunk.arrays()[1].as_ref();
        // println!("{arr:?}");
        Ok(())
    }
}
