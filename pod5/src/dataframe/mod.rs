//! `polars` DataFrame API for POD5 files
//!
//! This module provides utilities for interacting with POD5 tables using a
//! DataFrame API from `polars`.

// Polars notes
// take_unchecked needs to support
// 1. Map => casting to list array
// 2. Smaller types, aka not Large*
// 3. Support Extension data types, at least casting down to regular type
use std::{
    collections::HashMap,
    io::{Cursor, Read, Seek, SeekFrom},
};

use polars::{
    error::PolarsError,
    frame::DataFrame,
    lazy::{dsl::GetOutput, frame::IntoLazy},
    prelude::{self as pl, Column, NamedFrom},
    series::Series,
};
use polars_arrow::{
    datatypes::Field,
    io::ipc::read::{FileReader, read_file_metadata},
};

pub(crate) mod compatibility;
pub(crate) mod schema;

use svb16::decode;

use crate::error::Pod5Error;

/// DataFrame wrapper for the POD5 Signal table.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SignalDataFrame(pub(crate) DataFrame);

impl SignalDataFrame {
    /// Adds a column with the signal decompressed into i16 as `col_name`
    ///
    /// Assumes that there are two columns, samples (u32) representing the
    /// number of signal measurements, and signal, representing the
    /// compressed signal data (binary)
    pub fn decompress_signal(self) -> Result<Self, Pod5Error> {
        let res = self
            .0
            .lazy()
            .with_column(
                pl::as_struct(vec![pl::col("samples"), pl::col("signal")])
                    .map(decompress_signal_series, GetOutput::default())
                    .alias("signal"),
            )
            .collect()
            .map(Self)?;
        Ok(res)
    }

    /// Convert i16 ADC signal data into f32 picoamps
    pub fn to_picoamps(mut self, calibration: &Calibration) -> Self {
        let adcs = self.0["read_id"]
            .str()
            .unwrap()
            .into_iter()
            .flatten()
            .map(|rid| calibration.0.get(rid).unwrap())
            .collect::<Vec<_>>();
        let offsets = Column::from(Series::from_iter(adcs.iter().map(|adc| adc.offset)));
        let scale = Column::from(Series::from_iter(adcs.iter().map(|adc| adc.scale)));
        let res = (&self.0["signal"] + &offsets).unwrap();
        let res = (res * scale).unwrap();
        self.0.with_column(res).unwrap();
        self
    }

    /// Convert f32 picoamps signal data into i16 ADC
    // TOOD Make sure to be able to call and still do the conversion for the writer
    #[allow(dead_code)]
    pub(crate) fn with_adc(mut self, calibration: &Calibration) -> Self {
        let adcs = self.0["read_id"]
            .str()
            .unwrap()
            .into_iter()
            .flatten()
            .map(|rid| calibration.0.get(rid).unwrap())
            .collect::<Vec<_>>();
        let offsets = Column::from(Series::from_iter(adcs.iter().map(|adc| adc.offset)));
        let scale = Column::from(Series::from_iter(adcs.iter().map(|adc| adc.scale)));
        let res = (&self.0["signal"] / &scale).unwrap();
        let res = (res - offsets)
            .unwrap()
            .cast(&pl::DataType::List(Box::new(pl::DataType::Int16)))
            .unwrap();
        self.0.with_column(res).unwrap();
        self
    }

    /// Get the inner `polars` DataFrame.
    pub fn into_inner(self) -> DataFrame {
        self.0
    }
}

pub struct SignalDataFrameIter {
    pub(crate) fields: Vec<Field>,
    pub(crate) table_reader: FileReader<Cursor<Vec<u8>>>,
}

impl SignalDataFrameIter {
    pub(crate) fn new<R: Read + Seek>(
        offset: u64,
        length: u64,
        file: &mut R,
    ) -> Result<Self, Pod5Error> {
        let (fields, table_reader) =
            read_to_dataframe(offset, length, Pod5Error::SignalTableMissing, file)?;
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
        df.map(|res| {
            res.map(SignalDataFrame)
                .and_then(|sdf| sdf.decompress_signal())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReadDataFrame(pub(crate) DataFrame);

impl ReadDataFrame {
    pub fn into_inner(self) -> polars::prelude::DataFrame {
        self.0
    }

    /// Convert the `read_id` column into UUID strings.
    ///
    /// By default, `read_id`s are in the binary representation of a UUID. Use
    /// this method if you want to read the view the UUID in ASCII. The
    /// `col_name` can be any existing column or a new column that will be
    /// added to the end of the DataFrame.
    pub fn parse_read_ids(self, col_name: &str) -> Result<Self, Pod5Error> {
        let res = self
            .0
            .lazy()
            .with_column(
                pl::col("read_id")
                    .map(parse_uuid_from_read_id, GetOutput::default())
                    .alias(col_name),
            )
            .collect()?;
        Ok(Self(res))
    }
}

pub struct ReadDataFrameIter {
    pub(crate) fields: Vec<Field>,
    pub(crate) table_reader: FileReader<Cursor<Vec<u8>>>,
}

impl ReadDataFrameIter {
    pub fn fields(&self) -> &[Field] {
        self.fields.as_ref()
    }

    pub(crate) fn new<R: Read + Seek>(
        offset: u64,
        length: u64,
        file: &mut R,
    ) -> Result<Self, Pod5Error> {
        let (fields, table_reader) =
            read_to_dataframe(offset, length, Pod5Error::ReadTableMissing, file)?;
        Ok(Self {
            fields,
            table_reader,
        })
    }

    pub fn into_calibration(self) -> Calibration {
        Calibration::from_read_dfs(self)
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

pub struct RunInfoDataFrameIter {
    pub(crate) fields: Vec<Field>,
    pub(crate) table_reader: FileReader<Cursor<Vec<u8>>>,
}

impl RunInfoDataFrameIter {
    pub(crate) fn new<R: Read + Seek>(
        offset: u64,
        length: u64,
        file: &mut R,
    ) -> Result<Self, Pod5Error> {
        let (fields, table_reader) =
            read_to_dataframe(offset, length, Pod5Error::RunInfoTableMissing, file)?;
        Ok(Self {
            fields,
            table_reader,
        })
    }
}

impl Iterator for RunInfoDataFrameIter {
    type Item = Result<RunInfoDataFrame, Pod5Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let df = get_next_df(&self.fields, &mut self.table_reader);
        df.map(|res| res.map(RunInfoDataFrame))
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct RunInfoDataFrame(pub(crate) DataFrame);

impl RunInfoDataFrame {
    pub fn into_inner(self) -> polars::prelude::DataFrame {
        self.0
    }
}

pub(crate) fn get_next_df(
    fields: &[Field],
    table_reader: &mut FileReader<Cursor<Vec<u8>>>,
) -> Option<Result<DataFrame, Pod5Error>> {
    // TODO: Remove unwrap and avoid Option since it
    // can hide conversion problems
    table_reader.next().map(|chunk| {
        chunk
            .map(|batch| {
                let mut acc = Vec::with_capacity(fields.len());
                for (arr, f) in batch.into_arrays().into_iter().zip(fields.iter()) {
                    let s = compatibility::array_to_series(f, arr);
                    acc.push(s);
                }

                polars::prelude::DataFrame::from_iter(acc)
            })
            .map_err(Pod5Error::PolarsError)
    })
}

pub(crate) type TableReader = (Vec<Field>, FileReader<Cursor<Vec<u8>>>);

pub(crate) fn read_to_dataframe<R: Read + Seek>(
    offset: u64,
    length: u64,
    err: Pod5Error,
    file: &mut R,
) -> Result<TableReader, Pod5Error> {
    let mut run_info_buf = vec![0u8; length as usize];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut run_info_buf)?;
    let mut run_info_buf = Cursor::new(run_info_buf);
    let metadata = read_file_metadata(&mut run_info_buf).map_err(|_| err)?;
    let fields = metadata.schema.iter().map(|f| f.1).cloned().collect();

    let signal_table = FileReader::new(run_info_buf, metadata, None, None);
    Ok((fields, signal_table))
}

pub(crate) fn parse_uuid_from_read_id(
    series: pl::Column,
) -> Result<Option<pl::Column>, PolarsError> {
    let read_ids = series
        .binary()
        .unwrap()
        .into_iter()
        .map(|bs: Option<&[u8]>| bs.map(|bbs| uuid::Uuid::from_slice(bbs).unwrap().to_string()))
        .collect::<Vec<_>>();
    Ok(Some(Column::from(Series::new(
        series.name().clone(),
        read_ids,
    ))))
}

pub(crate) fn decompress_signal_series(
    sample_signal: Column,
) -> Result<Option<Column>, PolarsError> {
    let sample_signal = sample_signal.struct_().unwrap().fields_as_series();
    let sample = sample_signal[0].u32().unwrap();
    let signal = sample_signal[1].binary().unwrap();
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
    Ok(Some(Column::from(Series::new("decompressed".into(), out))))
}

#[derive(Debug)]
pub(crate) struct AdcData {
    pub(crate) offset: f32,
    pub(crate) scale: f32,
}

#[derive(Debug)]
pub struct Calibration(pub(crate) HashMap<String, AdcData>);

impl Calibration {
    fn from_read_dfs(iter: ReadDataFrameIter) -> Self {
        let mut cal_data = HashMap::new();
        for read_df in iter.flatten() {
            let df = read_df
                .0
                .select(["read_id", "calibration_offset", "calibration_scale"])
                .unwrap();
            let iters = df.iter().collect::<Vec<_>>();
            for (read_id, offset, scale) in itertools::multizip((
                iters[0].str().unwrap().into_iter().flatten(),
                iters[1].f32().unwrap().into_iter().flatten(),
                iters[2].f32().unwrap().into_iter().flatten(),
            )) {
                cal_data.insert(read_id.to_string(), AdcData { offset, scale });
            }
        }
        Calibration(cal_data)
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use crate::reader::Reader;

    #[test]
    fn test_reader() -> eyre::Result<()> {
        let path = "../extra/multi_fast5_zip_v3.pod5";
        let file = File::open(path)?;
        let mut reader = Reader::from_reader(file)?;
        for read_df in reader.read_dfs()?.flatten() {
            println!("{:?}", read_df.0.schema());
            let df = read_df.0.head(Some(4));
            println!("{df:?}");
        }
        let cal = reader.read_dfs()?.into_calibration();

        for signal_df in reader.signal_dfs()?.flatten() {
            let df = signal_df.to_picoamps(&cal).0.head(Some(4));
            println!("{df:?}");
        }
        for run_info_df in reader.run_info_dfs()?.flatten() {
            let schema = run_info_df.0.schema();
            println!("{schema:?}");
        }

        let signal_df = reader.signal_dfs()?.flatten().next().unwrap();
        println!("{:?}", signal_df.to_picoamps(&cal));
        Ok(())
    }
}
