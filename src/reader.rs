use std::io::{Cursor, Read, Seek, SeekFrom};

use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use polars::prelude as pl;
use polars::{
    error::PolarsError,
    frame::DataFrame,
    lazy::{dsl::GetOutput, frame::IntoLazy},
    prelude::NamedFrom,
    series::Series,
};
use polars_arrow::datatypes::Field;

use crate::{
    convert_array,
    error::Pod5Error,
    footer::{ParsedFooter, Table},
    svb16::decode,
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
}

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
        Ok(Self { reader, footer })
    }

    pub(crate) fn signal_dfs(&mut self) -> Result<SignalDataFrameIter, Pod5Error> {
        let table = self.footer.signal_table()?;
        let offset = table.as_ref().offset() as u64;
        let length = table.as_ref().length() as u64;
        let iter = SignalDataFrameIter::new(offset, length, &mut self.reader)?;
        Ok(iter)
    }

    pub(crate) fn read_dfs(&mut self) -> Result<ReadDataFrameIter, Pod5Error> {
        let table = self.footer.read_table()?;
        let offset = table.as_ref().offset() as u64;
        let length = table.as_ref().length() as u64;
        let iter = ReadDataFrameIter::new(offset, length, &mut self.reader)?;
        Ok(iter)
    }
}

#[derive(Debug)]
struct SignalDataFrame(DataFrame);

impl SignalDataFrame {
    /// Adds a column with the signal decompressed into i16
    ///
    /// Assumes that there are two columns, samples (u32) representing the number of signal measurements, and signal,
    /// representing the compressed signal data (binary)
    fn decompress_signal(self, col_name: &str) -> Result<Self, PolarsError> {
        self.0
            .lazy()
            .with_column(
                pl::as_struct(vec![pl::col("samples"), pl::col("signal")])
                    .map(decompress_signal, GetOutput::default())
                    .alias(col_name),
            )
            .collect()
            .map(Self)
    }

    fn into_inner(self) -> DataFrame {
        self.0
    }
}

struct SignalDataFrameIter {
    fields: Vec<Field>,
    table_reader: polars_arrow::io::ipc::read::FileReader<Cursor<Vec<u8>>>,
}

impl SignalDataFrameIter {
    fn new<R: Read + Seek>(offset: u64, length: u64, file: &mut R) -> Result<Self, Pod5Error> {
        let mut buf = vec![0u8; length as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf)?;
        let mut run_info_buf = Cursor::new(buf);
        let metadata = polars_arrow::io::ipc::read::read_file_metadata(&mut run_info_buf)
            .map_err(|_| Pod5Error::SignalTableMissing)?;
        let fields = metadata.schema.fields.clone();

        let table_reader =
            polars_arrow::io::ipc::read::FileReader::new(run_info_buf, metadata, None, None);
        Ok(Self {
            fields,
            table_reader,
        })
    }
}

impl Iterator for SignalDataFrameIter {
    type Item = Result<SignalDataFrame, Pod5Error>;

    /// TODO: Check when Result happens
    fn next(&mut self) -> Option<Self::Item> {
        let df = get_next_df(&self.fields, &mut self.table_reader);
        df.map(|res| res.map(SignalDataFrame))
    }
}

#[derive(Debug)]
struct ReadDataFrame(DataFrame);

struct ReadDataFrameIter {
    fields: Vec<Field>,
    table_reader: polars_arrow::io::ipc::read::FileReader<Cursor<Vec<u8>>>,
}

impl ReadDataFrameIter {
    fn new<R: Read + Seek>(offset: u64, length: u64, file: &mut R) -> Result<Self, Pod5Error> {
        let (fields, table_reader) =
            read_to_dataframe(offset, length, Pod5Error::ReadTableMissing, file)?;
        Ok(Self {
            fields,
            table_reader,
        })
    }
}

impl Iterator for ReadDataFrameIter {
    type Item = Result<ReadDataFrame, Pod5Error>;

    /// TODO: Check when Result happens
    fn next(&mut self) -> Option<Self::Item> {
        let df = get_next_df(&self.fields, &mut self.table_reader);
        df.map(|res| res.map(ReadDataFrame))
    }
}

fn get_next_df(
    fields: &[Field],
    table_reader: &mut polars_arrow::io::ipc::read::FileReader<Cursor<Vec<u8>>>,
) -> Option<Result<DataFrame, Pod5Error>> {
    if let Ok(chunk) = table_reader.next()? {
        let mut acc = Vec::with_capacity(fields.len());
        for (arr, f) in chunk.arrays().iter().zip(fields.iter()) {
            let arr = convert_array(arr.as_ref());
            let s = polars::prelude::Series::try_from((f, arr));
            acc.push(s.unwrap());
        }

        let df = polars::prelude::DataFrame::from_iter(acc);
        Some(Ok(df))
    } else {
        None
    }
}

type TableReader = (
    Vec<Field>,
    polars_arrow::io::ipc::read::FileReader<Cursor<Vec<u8>>>,
);

fn read_to_dataframe<R: Read + Seek>(
    offset: u64,
    length: u64,
    err: Pod5Error,
    file: &mut R,
) -> Result<TableReader, Pod5Error> {
    let mut run_info_buf = vec![0u8; length as usize];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut run_info_buf)?;
    let mut run_info_buf = Cursor::new(run_info_buf);
    let metadata =
        polars_arrow::io::ipc::read::read_file_metadata(&mut run_info_buf).map_err(|_| err)?;
    let fields = metadata.schema.fields.clone();

    let signal_table =
        polars_arrow::io::ipc::read::FileReader::new(run_info_buf, metadata, None, None);
    Ok((fields, signal_table))
}

fn combine_signal_rows(series: Series) -> Result<Option<Series>, PolarsError> {
    let xs = series
        .binary()
        .unwrap()
        .into_iter()
        .fold(Vec::new(), |mut acc: Vec<u8>, xs| {
            if let Some(xs) = xs {
                acc.extend_from_slice(xs);
                acc
            } else {
                acc
            }
        });
    Ok(Some(Series::new(series.name(), &xs)))
}

fn read_id_binary_to_str(series: Series) -> Result<Option<Series>, PolarsError> {
    let read_ids = series
        .binary()
        .unwrap()
        .into_iter()
        .map(|bs: Option<&[u8]>| bs.map(|bbs| uuid::Uuid::from_slice(bbs).unwrap().to_string()))
        .collect::<Vec<_>>();
    Ok(Some(Series::new(series.name(), read_ids)))
}

fn decompress_signal(sample_signal: Series) -> Result<Option<Series>, PolarsError> {
    let sample_signal = sample_signal.struct_().unwrap();
    let sample = sample_signal.fields()[0].u32().unwrap();
    let signal = sample_signal.fields()[1].binary().unwrap();
    let out = sample
        .into_iter()
        .zip(signal)
        .map(|(sa, si)| {
            let sa = sa.unwrap();
            let si = si.unwrap(); // Vec<u8>
            let decoded = decode(si, sa as usize).unwrap();
            Series::from_iter(decoded)
        })
        .collect::<Vec<_>>();
    Ok(Some(Series::new("decompressed", out)))
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use super::*;
    use polars::{lazy::dsl::GetOutput, prelude::IntoLazy};

    use polars::prelude as pl;

    #[test]
    fn test_reader() -> eyre::Result<()> {
        let file = File::open("extra/multi_fast5_zip_v3.pod5")?;
        let mut reader = Reader::from_reader(file)?;

        let mut reads = reader.read_dfs()?;
        let read_df = reads.next().unwrap().unwrap();
        println!("{read_df:?}");
        let read = read_df.0.column("signal").unwrap();
        println!("{read:?}");
        let first_read_idxs = read.list().unwrap().get_as_series(0).unwrap();
        println!("{first_read_idxs:?}");
        let first_read_idxs: Vec<u64> = first_read_idxs
            .u64()
            .unwrap()
            .into_iter()
            .map(|x| x.unwrap())
            .collect();

        let mut signals = reader.signal_dfs()?;
        let signal_df = signals.next().unwrap().unwrap();

        println!("{signal_df:?}");
        let x = signal_df
            .0
            .lazy()
            .select([
                pl::col("read_id")
                    .map(read_id_binary_to_str, GetOutput::default())
                    .alias("read_id"),
                pl::col("samples"),
                pl::as_struct(vec![pl::col("samples"), pl::col("signal")])
                    .map(decompress_signal, GetOutput::default())
                    .alias("decompressed"),
            ])
            .collect();
        println!("{x:?}");
        Ok(())
    }
}
