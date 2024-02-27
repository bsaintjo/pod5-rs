//! `polars` DataFrame API for POD5 files
//!
//! This module provides utilities for interacting with POD5 tables using a DataFrame API from `polars`.
use std::io::{Cursor, Read, Seek, SeekFrom};

use polars::{
    error::PolarsError,
    frame::DataFrame,
    lazy::{dsl::GetOutput, frame::IntoLazy},
    prelude as pl,
    prelude::NamedFrom,
    series::Series,
};
use polars_arrow::{
    array::{Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, NullArray},
    compute::cast::{binary_large_to_binary, fixed_size_binary_binary},
    datatypes::Field,
    io::ipc::read::{read_file_metadata, FileReader},
};

use crate::{error::Pod5Error, svb16::decode};

/// DataFrame wrapper for the POD5 Signal table.
#[derive(Debug)]
pub struct SignalDataFrame(pub(crate) DataFrame);

impl SignalDataFrame {
    /// Adds a column with the signal decompressed into i16 as `col_name`
    ///
    /// Assumes that there are two columns, samples (u32) representing the
    /// number of signal measurements, and signal, representing the
    /// compressed signal data (binary)
    pub fn decompress_signal(self, col_name: &str) -> Result<Self, Pod5Error> {
        let res = self
            .0
            .lazy()
            .with_column(
                pl::as_struct(vec![pl::col("samples"), pl::col("signal")])
                    .map(decompress_signal_series, GetOutput::default())
                    .alias(col_name),
            )
            .collect()
            .map(Self)?;
        Ok(res)
    }

    pub(crate) fn decompress_signal_with(
        self,
        col_name: &str,
        signal_col: &str,
        samples_col: &str,
    ) -> Result<Self, Pod5Error> {
        todo!()
    }

    /// Convert the `read_id` column into UUID strings.
    fn parse_read_ids(self, col_name: &str) -> Result<Self, Pod5Error> {
        todo!()
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
        let mut buf = vec![0u8; length as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf)?;
        let mut run_info_buf = Cursor::new(buf);
        let metadata =
            read_file_metadata(&mut run_info_buf).map_err(|_| Pod5Error::SignalTableMissing)?;
        let fields = metadata.schema.fields.clone();

        let table_reader = FileReader::new(run_info_buf, metadata, None, None);
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
pub struct ReadDataFrame(pub(crate) DataFrame);

impl ReadDataFrame {
    pub fn into_inner(self) -> polars::prelude::DataFrame {
        self.0
    }

    /// Convert the `read_id` column into UUID strings.
    fn parse_read_ids(self, col_name: &str) -> Result<Self, Pod5Error> {
        todo!()
    }
}

pub struct ReadDataFrameIter {
    pub(crate) fields: Vec<Field>,
    pub(crate) table_reader: FileReader<Cursor<Vec<u8>>>,
}

impl ReadDataFrameIter {
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
}

impl Iterator for ReadDataFrameIter {
    type Item = Result<ReadDataFrame, Pod5Error>;

    /// TODO: Check when Result happens
    fn next(&mut self) -> Option<Self::Item> {
        let df = get_next_df(&self.fields, &mut self.table_reader);
        df.map(|res| res.map(ReadDataFrame))
    }
}

pub(crate) fn get_next_df(
    fields: &[Field],
    table_reader: &mut FileReader<Cursor<Vec<u8>>>,
) -> Option<Result<DataFrame, Pod5Error>> {
    if let Ok(chunk) = table_reader.next()? {
        let mut acc = Vec::with_capacity(fields.len());
        for (arr, f) in chunk.into_arrays().into_iter().zip(fields.iter()) {
            let arr = convert_array2(arr);
            let s = polars::prelude::Series::try_from((f, arr));
            acc.push(s.unwrap());
        }

        let df = polars::prelude::DataFrame::from_iter(acc);
        Some(Ok(df))
    } else {
        None
    }
}

pub(crate) type TableReader = (
    Vec<Field>,
    polars_arrow::io::ipc::read::FileReader<Cursor<Vec<u8>>>,
);

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
    let metadata =
        polars_arrow::io::ipc::read::read_file_metadata(&mut run_info_buf).map_err(|_| err)?;
    let fields = metadata.schema.fields.clone();

    let signal_table =
        polars_arrow::io::ipc::read::FileReader::new(run_info_buf, metadata, None, None);
    Ok((fields, signal_table))
}

pub(crate) fn combine_signal_rows(series: Series) -> Result<Option<Series>, PolarsError> {
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

pub(crate) fn parse_uuid_from_read_id(
    series: pl::Series,
) -> Result<Option<pl::Series>, PolarsError> {
    let read_ids = series
        .binary()
        .unwrap()
        .into_iter()
        .map(|bs: Option<&[u8]>| bs.map(|bbs| uuid::Uuid::from_slice(bbs).unwrap().to_string()))
        .collect::<Vec<_>>();
    Ok(Some(Series::new(series.name(), read_ids)))
}

pub(crate) fn decompress_signal_series(
    sample_signal: Series,
) -> Result<Option<Series>, PolarsError> {
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

pub(crate) fn convert_array2(arr: Box<dyn Array>) -> Box<dyn Array> {
    let dt = arr.data_type();
    if dt == dt.to_logical_type() {
        arr
    } else {
        // field.data_type = dt.to_logical_type().clone();
        match dt.to_logical_type() {
            pl::ArrowDataType::Null => {
                let conc: &NullArray = arr.as_any().downcast_ref().unwrap();
                conc.to_boxed()
            }
            pl::ArrowDataType::Boolean => {
                let conc: &BooleanArray = arr.as_any().downcast_ref().unwrap();
                conc.to_boxed()
            }
            // ArrowDataType::Int8 => todo!(),
            // ArrowDataType::Int16 => todo!(),
            // ArrowDataType::Int32 => todo!(),
            // ArrowDataType::Int64 => todo!(),
            // ArrowDataType::UInt8 => todo!(),
            // ArrowDataType::UInt16 => todo!(),
            // ArrowDataType::UInt32 => todo!(),
            // ArrowDataType::UInt64 => todo!(),
            // ArrowDataType::Float16 => todo!(),
            // ArrowDataType::Float32 => todo!(),
            // ArrowDataType::Float64 => todo!(),
            // ArrowDataType::Timestamp(_, _) => todo!(),
            // ArrowDataType::Date32 => todo!(),
            // ArrowDataType::Date64 => todo!(),
            // ArrowDataType::Time32(_) => todo!(),
            // ArrowDataType::Time64(_) => todo!(),
            // ArrowDataType::Duration(_) => todo!(),
            // ArrowDataType::Interval(_) => todo!(),
            // ArrowDataType::Binary => todo!(),
            pl::ArrowDataType::FixedSizeBinary(_) => {
                let conc: &FixedSizeBinaryArray = arr.as_any().downcast_ref().unwrap();
                let conc = FixedSizeBinaryArray::new(
                    pl::ArrowDataType::FixedSizeBinary(16),
                    conc.values().clone(),
                    conc.validity().cloned(),
                );
                conc.to_boxed()
            }
            pl::ArrowDataType::LargeBinary => {
                let conc: &BinaryArray<i64> = arr.as_any().downcast_ref().unwrap();
                let conc = BinaryArray::new(
                    pl::ArrowDataType::LargeBinary,
                    conc.offsets().clone(),
                    conc.values().clone(),
                    conc.validity().cloned(),
                );
                conc.to_boxed()
            }
            // ArrowDataType::Utf8 => todo!(),
            // ArrowDataType::LargeUtf8 => todo!(),
            // ArrowDataType::List(_) => todo!(),
            // ArrowDataType::FixedSizeList(_, _) => todo!(),
            // ArrowDataType::LargeList(_) => todo!(),
            // ArrowDataType::Struct(_) => todo!(),
            // ArrowDataType::Union(_, _, _) => todo!(),
            // ArrowDataType::Map(_, _) => todo!(),
            // ArrowDataType::Dictionary(_, _, _) => todo!(),
            // ArrowDataType::Decimal(_, _) => todo!(),
            // ArrowDataType::Decimal256(_, _) => todo!(),
            // ArrowDataType::Extension(_, _, _) => unreachable!(),
            // ArrowDataType::BinaryView => todo!(),
            // ArrowDataType::Utf8View => todo!(),
            _ => unimplemented!(),
        }
    }
}

/// Convert Array into a compatible
pub(crate) fn convert_array(arr: &dyn Array) -> Box<dyn Array> {
    let mut dt = arr.data_type().clone();
    if let pl::ArrowDataType::Extension(_, pt, _) = dt {
        dt = *pt;
    }
    match dt {
        // ArrowDataType::Null => todo!(),
        // ArrowDataType::Boolean => todo!(),
        // ArrowDataType::Int8 => todo!(),
        // ArrowDataType::Int16 => todo!(),
        // ArrowDataType::Int32 => todo!(),
        // ArrowDataType::Int64 => todo!(),
        // ArrowDataType::UInt8 => todo!(),
        // ArrowDataType::UInt16 => todo!(),
        // ArrowDataType::UInt32 => todo!(),
        // ArrowDataType::UInt64 => todo!(),
        // ArrowDataType::Float16 => todo!(),
        // ArrowDataType::Float32 => todo!(),
        // ArrowDataType::Float64 => todo!(),
        // ArrowDataType::Timestamp(_, _) => todo!("{dt:?}"),
        // ArrowDataType::Date32 => todo!(),
        // ArrowDataType::Date64 => todo!(),
        // ArrowDataType::Time32(_) => todo!(),
        // ArrowDataType::Time64(_) => todo!(),
        // ArrowDataType::Duration(_) => todo!(),
        pl::ArrowDataType::Interval(_) => todo!("{dt:?}"),
        // ArrowDataType::Binary => todo!("{dt:?}"),
        pl::ArrowDataType::FixedSizeBinary(_) => {
            let c1: &FixedSizeBinaryArray = arr.as_any().downcast_ref().unwrap();
            let dt = pl::ArrowDataType::Binary;
            let c1: BinaryArray<i32> = fixed_size_binary_binary(c1, dt);
            c1.boxed()
        }
        pl::ArrowDataType::LargeBinary => {
            let c1: &BinaryArray<i64> = arr.as_any().downcast_ref().unwrap();
            let dt = pl::ArrowDataType::Binary;
            let c1: BinaryArray<i32> = binary_large_to_binary(c1, dt).unwrap();
            c1.boxed()
        }
        // ArrowDataType::Utf8 => todo!(),
        pl::ArrowDataType::LargeUtf8 => todo!("{dt:?}"),
        // ArrowDataType::List(_) => todo!("{dt:?}"),
        // ArrowDataType::FixedSizeList(_, _) => todo!(),
        pl::ArrowDataType::LargeList(_) => todo!("{dt:?}"),
        pl::ArrowDataType::Struct(_) => todo!("{dt:?}"),
        pl::ArrowDataType::Union(_, _, _) => todo!("{dt:?}"),
        // ArrowDataType::Map(_, _) => todo!(),
        // ArrowDataType::Dictionary(_, _, _) => todo!(),
        pl::ArrowDataType::Decimal(_, _) => todo!("{dt:?}"),
        pl::ArrowDataType::Decimal256(_, _) => todo!("{dt:?}"),
        pl::ArrowDataType::Extension(_, _, _) => unreachable!(),
        pl::ArrowDataType::BinaryView => todo!("{dt:?}"),
        // ArrowDataType::Utf8View => todo!(),
        _ => arr.to_boxed(),
    }
}
