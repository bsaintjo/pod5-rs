use std::{
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path,
};

use arrow2::io::ipc::read::{read_file_metadata, FileReader};
use polars::frame::DataFrame;
use polars_arrow::datatypes::Field;

use crate::{
    convert_array,
    error::Pod5Error,
    footer::{ParsedFooter, ReadTable, SignalTable, Table},
    read_table::SignalIdxs,
    run_info::RunInfoTable,
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

    pub(crate) fn read_table(&self) -> Result<DataFrame, Pod5Error> {
        let table = self.footer.read_table()?;
        todo!()
    }
}

struct SignalDataFrame(DataFrame);

struct SignalDataFrameIter {
    fields: Vec<Field>,
    table_reader: polars_arrow::io::ipc::read::FileReader<Cursor<Vec<u8>>>,
}

impl SignalDataFrameIter {
    fn new(offset: u64, length: u64, mut file: &File) -> Result<Self, Pod5Error> {
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

fn read_to_dataframe(offset: u64, length: u64, mut file: &File) -> Result<(), Pod5Error> {
    let mut run_info_buf = vec![0u8; length as usize];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut run_info_buf)?;
    let mut run_info_buf = Cursor::new(run_info_buf);
    let metadata = polars_arrow::io::ipc::read::read_file_metadata(&mut run_info_buf)
        .map_err(|_| Pod5Error::SignalTableMissing)?;
    let fields = metadata.schema.fields.clone();

    let signal_table =
        polars_arrow::io::ipc::read::FileReader::new(run_info_buf, metadata, None, None);
    for table in signal_table {
        if let Ok(chunk) = table {
            let mut acc = Vec::new();
            for (arr, f) in chunk.arrays().iter().zip(fields.iter()) {
                let arr = convert_array(arr.as_ref());
                let s = polars::prelude::Series::try_from((f, arr));
                acc.push(s.unwrap());
            }

            let df = polars::prelude::DataFrame::from_iter(acc.into_iter());
            println!("runinfo {df}");
        } else {
            println!("Error read table!")
        }
    }
    Ok(())
}

#[cfg(test)]
mod test {
    // use super::*;

    // #[test]
    // fn test_reader() -> eyre::Result<()> {
    //     let file = File::open("extra/multi_fast5_zip_v3.pod5")?;
    //     let mut reader = Reader::from_reader(file)?;
    //     let embedded_file = read_embedded_arrow(&mut reader.reader, reader.read_table)?;
    //     println!("{:?}", embedded_file.schema());
    //     // for _read in reader.reads() {
    //     //     todo!()
    //     // }
    //     Ok(())
    // }
}
