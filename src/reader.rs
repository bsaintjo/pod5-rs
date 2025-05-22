//! Reading from a POD5 file.
use std::io::{Read, Seek, SeekFrom};

use crate::{
    dataframe::{ReadDataFrameIter, RunInfoDataFrameIter, SignalDataFrameIter},
    error::Pod5Error,
    footer::ParsedFooter,
};

const FILE_SIGNATURE: [u8; 8] = [0x8b, b'P', b'O', b'D', b'\r', b'\n', 0x1a, b'\n'];

fn valid_signature<R>(mut reader: R) -> Result<bool, std::io::Error>
where
    R: Read + Seek,
{
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(buf == FILE_SIGNATURE)
}

pub struct Reader<R> {
    pub(crate) reader: R,
    pub(crate) footer: ParsedFooter,
}

impl<R> Reader<R>
where
    R: Read + Seek,
{
    pub fn from_reader(mut reader: R) -> Result<Self, Pod5Error> {
        if !valid_signature(&mut reader)? {
            return Err(Pod5Error::SignatureFailure("Start"));
        }
        reader.seek(SeekFrom::End(-8))?;
        if !valid_signature(&mut reader)? {
            return Err(Pod5Error::SignatureFailure("End"));
        }
        let footer = ParsedFooter::read_footer(&mut reader)?;
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

    use polars::{lazy::dsl::GetOutput, prelude as pl, prelude::IntoLazy};

    use super::*;
    use crate::dataframe::{decompress_signal_series, parse_uuid_from_read_id};

    #[test]
    fn test_reader() -> eyre::Result<()> {
        let file = File::open("extra/multi_fast5_zip_v3.pod5")?;
        let mut reader = Reader::from_reader(file)?;

        let mut reads = reader.read_dfs()?;
        let read_df = reads.next().unwrap().unwrap();
        println!("{read_df:?}");
        let read = read_df.0.column("signal").unwrap();
        println!("{read:?}");

        let mut signals = reader.signal_dfs()?;
        let signal_df = signals.next().unwrap().unwrap();

        println!("{signal_df:?}");
        let x = signal_df
            .0
            .lazy()
            // .select([
            //     pl::col("read_id")
            //         .map(parse_uuid_from_read_id, GetOutput::default())
            //         .alias("read_id"),
            //     pl::col("samples"),
            //     pl::as_struct(vec![pl::col("samples"), pl::col("signal")])
            //         .map(decompress_signal_series, GetOutput::default())
            //         .alias("decompressed"),
            // ])
            .collect();
        println!("{x:?}");
        Ok(())
    }
}
