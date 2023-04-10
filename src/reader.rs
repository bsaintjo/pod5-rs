use std::{
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use arrow2::io::ipc::read::{read_file_metadata, FileReader};

use crate::{
    error::Pod5Error,
    footer::{ParsedFooter, ReadTable, SignalTable, Table},
    run_info::RunInfoTable, read_table::SignalIdxs,
};

pub(crate) fn read_embedded_arrow<R, T>(
    mut file: R,
    efile: T,
) -> eyre::Result<FileReader<Cursor<Vec<u8>>>>
where
    R: Read + Seek,
    T: AsRef<Table>,
{
    let offset = efile.as_ref().offset() as u64;
    let length = efile.as_ref().length() as u64;
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

pub(crate) struct Reader<R> {
    pub(crate) reader: R,
    pub(crate) footer: ParsedFooter,
    pub(crate) read_table: ReadTable,
    pub(crate) signal_table: SignalTable,
    pub(crate) run_info_table: RunInfoTable,
}

// Maybe more useful for mmap related stuff
impl<'a, R> Reader<R> where R: AsRef<&'a [u8]> {}

impl<R> Reader<R>
where
    R: Read + Seek,
{
    pub(crate) fn from_reader(mut reader: R) -> Result<Self, Pod5Error> {
        if !valid_signature(&mut reader)? {
            return Err(Pod5Error::SignatureFailure("Start"));
        }
        reader.seek(SeekFrom::End(-8))?;
        if !valid_signature(&mut reader)? {
            return Err(Pod5Error::SignatureFailure("End"));
        }
        let footer = ParsedFooter::read_footer(&mut reader)?;
        let signal_table = footer.signal_table()?;
        let read_table = footer.read_table()?;
        let run_info_table = footer.run_info_table()?;
        Ok(Self {
            reader,
            footer,
            read_table,
            signal_table,
            run_info_table,
        })
    }

    fn reads(&self) -> ReadIter {
        todo!()
    }
}


struct ReadIter {
    idxs: SignalIdxs,
}

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

    fn run_info(&self) -> Option<RunInfoTable> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_reader() -> eyre::Result<()> {
        let file = File::open("extra/multi_fast5_zip_v3.pod5")?;
        let mut reader = Reader::from_reader(file)?;
        let embedded_file = read_embedded_arrow(&mut reader.reader, reader.read_table)?;
        println!("{:?}", embedded_file.schema());
        // for _read in reader.reads() {
        //     todo!()
        // }
        Ok(())
    }
}
