//! Reading from a POD5 file.
use std::io::{Read, Seek, SeekFrom};

use pod5_format::{ParsedFooter, valid_signature};

use crate::{
    dataframe::{ReadDataFrameIter, RunInfoDataFrameIter, SignalDataFrameIter},
    error::Pod5Error,
};

pub struct Reader<R> {
    pub(crate) reader: R,
    pub(crate) footer: ParsedFooter,
}

impl<R> Reader<R>
where
    R: Read + Seek,
{
    pub fn from_reader(mut reader: R) -> Result<Self, Pod5Error> {
        let mut signature = [0u8; 8];
        reader.read_exact(&mut signature)?;
        if !valid_signature(&mut signature) {
            return Err(Pod5Error::SignatureFailure("Start"));
        }
        reader.seek(SeekFrom::End(-8))?;
        reader.read_exact(&mut signature)?;
        if !valid_signature(&mut signature) {
            return Err(Pod5Error::SignatureFailure("End"));
        }
        let mut footer_bytes = Vec::new();
        reader.read_to_end(&mut footer_bytes)?;
        let footer = ParsedFooter::read_footer(&mut footer_bytes)?;
        Ok(Self { reader, footer })
    }

    pub fn signal_dfs(&mut self) -> Result<SignalDataFrameIter, Pod5Error> {
        let table = self.footer.signal_table()?;
        let offset = table.as_ref().offset() as u64;
        let length = table.as_ref().length() as u64;
        let iter = SignalDataFrameIter::new(offset, length, &mut self.reader)?;
        Ok(iter)
    }

    pub fn read_dfs(&mut self) -> Result<ReadDataFrameIter, Pod5Error> {
        let table = self.footer.read_table()?;
        let offset = table.as_ref().offset() as u64;
        let length = table.as_ref().length() as u64;
        let iter = ReadDataFrameIter::new(offset, length, &mut self.reader)?;
        Ok(iter)
    }

    pub fn run_info_dfs(&mut self) -> Result<RunInfoDataFrameIter, Pod5Error> {
        let table = self.footer.run_info_table()?;
        let offset = table.as_ref().offset() as u64;
        let length = table.as_ref().length() as u64;
        let iter = RunInfoDataFrameIter::new(offset, length, &mut self.reader)?;
        Ok(iter)
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use polars::prelude::IntoLazy;

    use super::*;

    #[test]
    fn test_reader() -> eyre::Result<()> {
        let file = File::open("../extra/multi_fast5_zip_v3.pod5")?;
        let mut reader = Reader::from_reader(file)?;

        let mut reads = reader.read_dfs()?;
        let read_df = reads.next().unwrap().unwrap();
        println!("{read_df:?}");
        let read = read_df.0.column("signal").unwrap();
        println!("{read:?}");

        let mut signals = reader.signal_dfs()?;
        let signal_df = signals.next().unwrap().unwrap();

        println!("{signal_df:?}");
        let x = signal_df.0.lazy().collect();
        println!("{x:?}");
        Ok(())
    }
}
