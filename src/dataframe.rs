//! `polars` DataFrame API for POD5 files
//!
//! This module provides utilities for interacting with POD5 tables using a DataFrame API from `polars`.
use std::io::{Cursor, Read, Seek, SeekFrom};

use polars::{
    datatypes::ArrowDataType,
    error::PolarsError,
    frame::DataFrame,
    lazy::{dsl::GetOutput, frame::IntoLazy},
    prelude::{self as pl, NamedFrom},
    series::Series,
};
use polars_arrow::{
    array::{
        Array, BinaryArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray, NullArray,
        PrimitiveArray, StructArray, Utf8Array,
    },
    bitmap::{Bitmap, MutableBitmap},
    compute::{
        cast::{
            binary_large_to_binary, dictionary_to_values, fixed_size_binary_binary,
            primitive_to_primitive, utf8_to_binary, utf8_to_large_utf8,
        },
        take::take_unchecked,
    },
    datatypes::Field,
    io::ipc::read::{read_file_metadata, FileReader},
    types::Index,
};

use crate::{error::Pod5Error, svb16::decode};

/// DataFrame wrapper for the POD5 Signal table.
#[derive(Debug, Clone, PartialEq, Default)]
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

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReadDataFrame(pub(crate) DataFrame);

impl ReadDataFrame {
    pub fn into_inner(self) -> polars::prelude::DataFrame {
        self.0
    }

    /// Convert the `read_id` column into UUID strings.
    ///
    /// By default, `read_id`s are in the binary representation of a UUID. Use
    /// this method if you want to read the view the UUID in ASCII. The `col_name` can
    /// be any existing column or a new column that will be added to the end of the DataFrame.
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
    let metadata = read_file_metadata(&mut run_info_buf).map_err(|_| err)?;
    let fields = metadata.schema.fields.clone();

    let signal_table = FileReader::new(run_info_buf, metadata, None, None);
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

/// Copied from polars_arrow because it isn't exported
///
/// Relevant code: https://docs.rs/polars-arrow/0.37.0/src/polars_arrow/compute/take/structure.rs.html#23
#[inline]
unsafe fn take_validity<I: Index>(
    validity: Option<&Bitmap>,
    indices: &PrimitiveArray<I>,
) -> Option<Bitmap> {
    let indices_validity = indices.validity();
    match (validity, indices_validity) {
        (None, _) => indices_validity.cloned(),
        (Some(validity), None) => {
            let iter = indices.values().iter().map(|index| {
                let index = index.to_usize();
                validity.get_bit_unchecked(index)
            });
            MutableBitmap::from_trusted_len_iter(iter).into()
        }
        (Some(validity), _) => {
            let iter = indices.iter().map(|x| match x {
                Some(index) => {
                    let index = index.to_usize();
                    validity.get_bit_unchecked(index)
                }
                None => false,
            });
            MutableBitmap::from_trusted_len_iter(iter).into()
        }
    }
}

// Helps convert dictionaries in POD5s to be compatible with polars
// polars_arrow::compute::take::take_unchecked only supports a handful of types so we need to manually
// manage the conversion.
//
// Relevant documentation:
// https://docs.rs/polars-arrow/0.37.0/src/polars_arrow/compute/take/structure.rs.html#50
// https://docs.rs/polars-arrow/0.37.0/src/polars_arrow/compute/take/mod.rs.html#72
pub(crate) fn convert_dictionaries(arr: Box<dyn Array>) -> Box<dyn Array> {
    let arr_dict = arr.as_any().downcast_ref::<DictionaryArray<i16>>().unwrap();
    let pl::ArrowDataType::Struct(..) = arr_dict.values().data_type() else {
        return arr;
    };
    let indices = primitive_to_primitive::<_, i64>(arr_dict.keys(), &pl::ArrowDataType::Int64);
    let s = arr_dict.values().as_any().downcast_ref::<StructArray>().unwrap();
    let pl::ArrowDataType::Struct(fields) = s.data_type().clone() else {
        unreachable!()
    };
    let mut new_fields = Vec::with_capacity(fields.capacity());
    let brr: Vec<Box<dyn Array>> = s
        .values()
        .iter()
        .zip(fields)
        .filter_map(|(a, mut f)| {
            let b = if a.data_type() == &pl::ArrowDataType::Utf8 {
                let conc = a.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                let res = utf8_to_large_utf8(conc);
                f.data_type = ArrowDataType::LargeUtf8;
                res.boxed()
            } else {
                a.to_boxed()
            };
            if let &pl::ArrowDataType::Map(..) = b.data_type() {
                None
            } else {
                new_fields.push(f);
                Some(unsafe { take_unchecked(b.as_ref(), &indices) })
            }
        })
        .collect();
    let validity = unsafe { take_validity(s.validity(), &indices) };
    StructArray::new(pl::ArrowDataType::Struct(new_fields), brr, validity).boxed()
}

pub(crate) fn convert_array2(arr: Box<dyn Array>) -> Box<dyn Array> {
    let dt = arr.data_type();
    if let pl::ArrowDataType::Dictionary(..) = dt {
        return convert_dictionaries(arr);
    }

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
