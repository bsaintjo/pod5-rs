use std::{
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use arrow2::io::ipc::read::{read_file_metadata, FileReader};

use crate::{
    error::Pod5Error,
    footer::{ParsedFooter, ReadTable, SignalTable},
    footer_generated::minknow::reads_format::EmbeddedFile, run_info::RunInfo,
};

pub(crate) fn read_embedded_arrow<R>(
    mut file: R,
    efile: &EmbeddedFile,
) -> eyre::Result<FileReader<Cursor<Vec<u8>>>>
where
    R: Read + Seek,
{
    let offset = efile.offset() as u64;
    let length = efile.length() as u64;
    let mut embedded_arrow = vec![0u8; length as usize];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut embedded_arrow)?;

    let mut signal_buf = Cursor::new(embedded_arrow);
    let metadata = read_file_metadata(&mut signal_buf)?;
    let table = FileReader::new(signal_buf, metadata, None, None);
    Ok(table)
}

const FILE_SIGNATURE: [u8; 8] = [0x8b, b'P', b'O', b'D', b'\r', b'\n', 0x1a, b'\n'];

fn valid_signature<R>(mut reader: R) -> Result<bool, std::io::Error>
where
    R: Read + Seek,
{
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(buf == FILE_SIGNATURE)
}

struct Reader<R> {
    file: R,
    footer: ParsedFooter,
    read_table_idx: Option<ReadTable>,
    signal_table_idx: Option<SignalTable>,
    run_info_table_idx: Option<usize>,
}

impl<R> Reader<R>
where
    R: Read + Seek,
{
    fn from_reader(mut reader: R) -> Result<Self, Pod5Error> {
        if !valid_signature(&mut reader)? {
            return Err(Pod5Error::SignatureFailure);
        }
        reader.seek(SeekFrom::End(-8))?;
        if valid_signature(&mut reader)? {
            return Err(Pod5Error::SignatureFailure);
        }
        let footer = ParsedFooter::read_footer(&mut reader)?;
        let signal_table_idx = footer.signal_table()?;
        let read_table = footer.read_table()?;
        todo!()
    }

    fn reads(&self) -> ReadIter {
        todo!()
    }
}

struct ReadIter;

impl Iterator for ReadIter {
    type Item = Pod5Read;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

struct Pod5Read;

impl Pod5Read {
    fn read_id(&self) -> &str {
        todo!()
    }

    fn sample_count(&self) -> usize {
        todo!()
    }

    fn run_info(&self) -> Option<RunInfo> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_reader() -> eyre::Result<()> {
        let file = File::open("extra/multi_fast5_zip_v0.pod5")?;
        let reader = Reader::from_reader(file)?;
        for _read in reader.reads() {
            todo!()
        }
        Ok(())
    }
}
