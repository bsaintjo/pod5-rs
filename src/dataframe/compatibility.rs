use std::{io::Write, marker::PhantomData};

use polars::{
    datatypes::ArrowDataType,
    prelude::{ArrowField, CompatLevel, LargeBinaryArray, PlSmallStr},
    series::Series,
};

use polars::prelude as pl;
use polars_arrow::{
    array::{
        Array, BinaryViewArray, FixedSizeBinaryArray, Float32Array, Int16Array, ListArray,
        MutableArray, MutableBinaryArray, MutableFixedSizeBinaryArray, Utf8Array, Utf8ViewArray,
    },
    compute::concatenate,
    datatypes::{ArrowSchemaRef, ExtensionType},
    io::ipc::write::FileWriter as ArrowFileWriter,
    record_batch::RecordBatchT,
};
use polars_schema::Schema;
use uuid::Uuid;

use crate::svb16;

/// Convert Arrow arrays into polars Series. This works for almost all arrays except the Extensions.
/// In order for properly handle Extension types, the arrays need to be cast to an Array type that polars can handle.
/// These are:
/// Extension(minknow.vbz) ("signal data") => LargeBinary, downstream the signal can be converted because the
/// samples columns is needed to decode
/// Extension(minknow.uuid) ("read id") => Utf8Array
///
/// WARNING: For adding additional support for types, Arrays can not just be recast into a supported concrete Array type,
/// instead, the inner components need to be removed, somewhere the Extension type still remains after casting, causing
/// confusing errors downstream.
/// Don't:
/// Extension(minknow.vbz) => downcast_ref into a LargeBinaryArray => box back directly to Box<dyn Array> => ComputeError
/// Instead:
/// Extension(minknow.vbz) => downcast_ref into LargeBinaryArray => into_inner and split into components (offsets, bitmap, etc.)
/// => LargeBinary::new with components => boxed to Box<dyn Array> => Series::try_from works properly
pub(crate) fn array_to_series(field: &pl::ArrowField, arr: Box<dyn Array>) -> Series {
    log::debug!("{field:?}");
    match Series::try_from((field, arr.clone())) {
        Ok(series) => return series,
        Err(e) => log::debug!("{e:?}, attempting conversion"),
    }

    match field.dtype() {
        // Read UUIDs
        ArrowDataType::Extension(bet)
            if bet.inner.to_logical_type() == &ArrowDataType::FixedSizeBinary(16) =>
        {
            let field =
                pl::ArrowField::new(PlSmallStr::from("minknow.uuid"), ArrowDataType::Utf8, true);
            let arr = arr
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .values_iter()
                .map(|x| Some(Uuid::from_slice(x).unwrap().to_string()))
                .collect::<Vec<_>>();
            let arr = Utf8Array::<i32>::from(arr);

            match Series::try_from((&field, arr.boxed())) {
                Ok(series) => series,
                Err(e) => {
                    panic!("{e:?}");
                }
            }
        }

        // Signal data
        ArrowDataType::Extension(bet)
            if bet.inner.to_logical_type() == &ArrowDataType::LargeBinary =>
        {
            let field = pl::ArrowField::new(
                PlSmallStr::from("minknow.vbz"),
                ArrowDataType::LargeBinary,
                true,
            );
            let (_, offsets, values, bitmap) = arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .clone()
                .into_inner();
            let conc =
                LargeBinaryArray::new(pl::ArrowDataType::LargeBinary, offsets, values, bitmap);

            match Series::try_from((&field, conc.boxed())) {
                Ok(series) => series,
                Err(e) => {
                    panic!("{e:?}");
                }
            }
        }
        _ => {
            panic!("unimplemented datatype: {:?}", field);
        }
    }
}

struct TableWriter<T, W: Write> {
    schema: ArrowSchemaRef,
    writer: ArrowFileWriter<W>,
    table: PhantomData<T>,
}

#[derive(Debug)]
struct FieldArray {
    field: pl::ArrowField,
    arr: Box<dyn Array>,
}

impl FieldArray {
    fn new(field: pl::ArrowField, arr: Box<dyn Array>) -> Self {
        Self { field, arr }
    }
}

fn field_arrs_to_schema(farrs: &[FieldArray]) -> Schema<pl::ArrowField> {
    farrs.iter().map(|farr| farr.field.clone()).collect()
}

fn field_arrs_to_record_batch(
    farrs: Vec<FieldArray>,
    schema: ArrowSchemaRef,
) -> RecordBatchT<Box<dyn Array>> {
    let height = farrs[0].arr.len();
    let arrays = farrs.into_iter().map(|f| f.arr).collect::<Vec<_>>();
    RecordBatchT::new(height, schema, arrays)
}

/// Converts a minknow.vbz array into a LargeBinary array.
///
/// The minknow.vbz can be a list[f32], list[i16], or list[u8](?) depending on how it was processed.
fn minknow_vbz_to_large_binary(
    field: &pl::ArrowField,
    chunks: Vec<Box<dyn Array>>,
) -> Result<FieldArray, polars::error::PolarsError> {
    let new_dt = ArrowDataType::Extension(Box::new(ExtensionType {
        name: field.name.clone(),
        inner: ArrowDataType::LargeBinary,
        metadata: None,
    }));
    let new_field = ArrowField::new(field.name.clone(), new_dt, true);
    let mut acc = MutableBinaryArray::<i64>::new();
    chunks.into_iter().for_each(|chunk| {
        match &field.dtype {
            ArrowDataType::BinaryView | ArrowDataType::LargeBinary => {
                let items = chunk
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .unwrap()
                    .iter()
                    .map(|s| s.map(|t| t.to_vec()));
                for a in items {
                    acc.push(a);
                }
            }

            // Decompressed signal data
            ArrowDataType::LargeList(inner)
                if matches!(
                    inner.as_ref(),
                    ArrowField {
                        dtype: ArrowDataType::Int16,
                        ..
                    }
                ) =>
            {
                chunk
                    .as_any()
                    .downcast_ref::<ListArray<i64>>()
                    .unwrap()
                    .iter()
                    .map(|picoamp_arr| {
                        picoamp_arr.map(|parr| {
                            let res = parr
                                .as_any()
                                .downcast_ref::<Int16Array>()
                                .unwrap()
                                .values()
                                .iter()
                                .copied()
                                .collect::<Vec<_>>();

                            svb16::encode(&res).unwrap()
                        })
                    })
                    .for_each(|item| acc.push(item));
            }

            // Decompressed signal picoamp data
            ArrowDataType::LargeList(inner)
                if matches!(
                    inner.as_ref(),
                    ArrowField {
                        dtype: ArrowDataType::Float32,
                        ..
                    }
                ) =>
            {
                let items = chunk
                    .as_any()
                    .downcast_ref::<ListArray<i64>>()
                    .unwrap()
                    .iter()
                    .map(|picoamp_arr| {
                        picoamp_arr.map(|parr| {
                            let res = parr
                                .as_any()
                                .downcast_ref::<Float32Array>()
                                .unwrap()
                                .values_iter()
                                .copied()
                                .collect::<Vec<_>>();
                            todo!()
                        })
                    });
                todo!()
                //     .unwrap()
                //     .values_iter()
                //     .map(|s| Some(s.as_bytes().to_vec()));
                // for a in items {
                //     acc.push(a);
                // }
            }
            _ => panic!("Invalid datatype for field: {field:?}"),
        };
    });
    Ok(FieldArray::new(new_field, acc.as_box()))
}

/// Convert a minknow.uuid array into a FixedSizeBinary array.
///
/// The minknow.uuid is usually a str column and we try to convert from different types of Utf8* arrays.
fn minknow_uuid_to_fixed_size_binary(
    field: &pl::ArrowField,
    chunks: Vec<Box<dyn Array>>,
) -> Result<FieldArray, polars::error::PolarsError> {
    let new_dt = ArrowDataType::Extension(Box::new(ExtensionType {
        name: field.name.clone(),
        inner: ArrowDataType::FixedSizeBinary(16),
        metadata: None,
    }));
    let new_field = ArrowField::new(field.name.clone(), new_dt, true);
    let mut acc = MutableFixedSizeBinaryArray::new(16);
    chunks.into_iter().for_each(|chunk| {
        match &field.dtype {
            ArrowDataType::Utf8View | ArrowDataType::LargeUtf8 => {
                chunk
                    .as_any()
                    .downcast_ref::<Utf8ViewArray>()
                    .unwrap()
                    .values_iter()
                    .map(|s| {
                        let res: [u8; 16] =
                            Vec::from(s.parse::<Uuid>().unwrap()).try_into().unwrap();
                        Some(res)
                    })
                    .for_each(|item| acc.push(item));
            }
            ArrowDataType::Utf8 => {
                chunk
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap()
                    .values_iter()
                    .map(|s| {
                        let res: [u8; 16] =
                            Vec::from(s.parse::<Uuid>().unwrap()).try_into().unwrap();
                        Some(res)
                    })
                    .for_each(|item| acc.push(item));
            }

            _ => {
                panic!("Invalid datatype for field: {field:?}");
            }
        };
    });
    let acc = acc.as_box();
    Ok(FieldArray::new(new_field, acc))
}

/// Main entrypoint for series conversion. By default it converts the arrays as is, but for columns that are converted by array_to_series,
/// we need to convert back to the original type used in POD5 tables.
fn series_to_array(series: Series) -> FieldArray {
    let name = series.name().clone();
    let field = series
        .dtype()
        .to_arrow_field(name.clone(), CompatLevel::newest());
    log::debug!("series_to_array: {field:?}");
    let chunks = series.into_chunks();
    log::debug!("example chunk: {:?}", &chunks[0]);
    match (field.name.as_str(), &field.dtype) {
        ("minknow.vbz", _) => minknow_vbz_to_large_binary(&field, chunks).unwrap(),
        ("minknow.uuid", _) => minknow_uuid_to_fixed_size_binary(&field, chunks).unwrap(),
        _ => {
            let chunks = chunks.iter().map(|b| b.as_ref()).collect::<Vec<_>>();
            let acc = concatenate::concatenate(&chunks).unwrap();
            let farr = FieldArray::new(field, acc);
            log::debug!("series_to_array: {farr:?}");
            farr
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use polars::{df, prelude::NamedFrom};
    use polars_arrow::io::ipc::write::WriteOptions;

    use crate::dataframe::{AdcData, Calibration, SignalDataFrame};

    use super::*;

    fn init() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();
    }

    #[test]
    fn test_series_to_array_minknow_uuid() {
        init();
        let example = String::from("67e55044-10b1-426f-9247-bb680e5fe0c8");
        let series = Series::new("minknow.uuid".into(), [example]);
        let farr = series_to_array(series);
    }

    #[test]
    fn test_series_to_array_minknow_vbz() {
        init();
        let example = Some(b"test" as &[u8]);
        let series = Series::new("minknow.vbz".into(), [example]);
        let farr = series_to_array(series);
    }

    #[test]
    fn test_series_to_array_samples() {
        init();
        let example = [Some(717u32), Some(123u32)];
        let series = Series::new("samples".into(), example);
        let farr = series_to_array(series);
    }

    #[test]
    fn test_writer() {
        init();
        let example = Some(b"test" as &[u8]);
        let series = Series::new("minknow.vbz".into(), [example]);
        let farr = vec![series_to_array(series)];
        let schema = Arc::new(field_arrs_to_schema(&farr));
        let chunk = field_arrs_to_record_batch(farr, schema.clone());

        let buf: Vec<u8> = Vec::new();
        let mut writer =
            ArrowFileWriter::try_new(buf, schema.clone(), None, WriteOptions::default()).unwrap();
        writer.write(&chunk, None).unwrap();
        writer.finish().unwrap();
    }

    #[test]
    fn test_df() {
        init();
        let cal = Calibration(
            [(
                "67e55044-10b1-426f-9247-bb680e5fe0c8".into(),
                AdcData {
                    offset: 0.0,
                    scale: 1.0,
                },
            )]
            .into_iter()
            .collect(),
        );
        let df = SignalDataFrame(df!("minknow.uuid" => ["67e55044-10b1-426f-9247-bb680e5fe0c8", "67e55044-10b1-426f-9247-bb680e5fe0c8"],
                                                          "minknow.vbz" => [[0.1f32, 0.2f32].iter().collect::<Series>(), [0.3f32, 0.4f32].iter().collect::<Series>()],
                                                          "samples" => [2u32, 2u32],
                                                         ).unwrap());
        println!("{df:?}");
        let df = df.with_adc(&cal);
        println!("{df:?}");
        let field_arrays =
            df.0.iter()
                .map(|s| series_to_array(s.clone()))
                .collect::<Vec<_>>();
        let schema = Arc::new(field_arrs_to_schema(&field_arrays));
        let chunk = field_arrs_to_record_batch(field_arrays, schema.clone());
        let buf: Vec<u8> = Vec::new();
        let mut writer =
            ArrowFileWriter::try_new(buf, schema.clone(), None, WriteOptions::default()).unwrap();
        writer.write(&chunk, None).unwrap();
        writer.finish().unwrap();
    }
}
