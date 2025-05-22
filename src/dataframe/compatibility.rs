use std::sync::Arc;

use polars::{
    datatypes::ArrowDataType,
    error::PolarsError,
    prelude::{self as pl, ArrowField, LargeBinaryArray, PlSmallStr},
    series::Series,
};
use polars_arrow::{
    array::{
        Array, BinaryArray, BinaryViewArray, FixedSizeBinaryArray, Float32Array, Int16Array,
        ListArray, MapArray, MutableArray, MutableBinaryArray, MutableFixedSizeBinaryArray,
        MutableListArray, MutablePrimitiveArray, MutableUtf8Array, PrimitiveArray, StructArray,
        TryPush, Utf8Array, Utf8ViewArray,
    },
    datatypes::{ArrowSchemaRef, ExtensionType},
    offset::OffsetsBuffer,
    record_batch::RecordBatchT,
};
use polars_schema::Schema;
use uuid::Uuid;

use super::SignalDataFrame;
use crate::{dataframe::schema::map_field, svb16};

/// Convert Arrow arrays into polars Series. This works for almost all arrays
/// except the Extensions. In order for properly handle Extension types, the
/// arrays need to be cast to an Array type that polars can handle. These are:
/// Extension(minknow.vbz) ("signal data") => LargeBinary, downstream the signal
/// can be converted because the samples columns is needed to decode
/// Extension(minknow.uuid) ("read id") => Utf8Array
///
/// WARNING: For adding additional support for types, Arrays can not just be
/// recast into a supported concrete Array type, instead, the inner components
/// need to be removed, somewhere the Extension type still remains after
/// casting, causing confusing errors downstream.
/// Don't:
/// Extension(minknow.vbz) => downcast_ref into a LargeBinaryArray => box back
/// directly to Box<dyn Array> => ComputeError Instead:
/// Extension(minknow.vbz) => downcast_ref into LargeBinaryArray => into_inner
/// and split into components (offsets, bitmap, etc.) => LargeBinary::new with
/// components => boxed to Box<dyn Array> => Series::try_from works properly
pub(crate) fn array_to_series(field: &pl::ArrowField, arr: Box<dyn Array>) -> Series {
    log::debug!("array_to_series: {field:?}");
    match Series::try_from((field, arr.clone())) {
        Ok(series) => return series,
        Err(e) => log::debug!("{e:?}, attempting conversion"),
    }

    match field.dtype() {
        // Read UUIDs
        ArrowDataType::Extension(bet)
            if bet.inner.to_logical_type() == &ArrowDataType::FixedSizeBinary(16) =>
        {
            let field = pl::ArrowField::new(PlSmallStr::from("read_id"), ArrowDataType::Utf8, true);
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
            let field =
                pl::ArrowField::new(PlSmallStr::from("signal"), ArrowDataType::LargeBinary, true);
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

struct Converter<T> {
    arr: Box<dyn Array>,
    field: pl::ArrowField,
    schema: ArrowSchemaRef,
    _marker: std::marker::PhantomData<T>,
}

impl Converter<SignalDataFrame> {
    fn new(arr: Box<dyn Array>, field: pl::ArrowField, schema: ArrowSchemaRef) -> Self {
        Self {
            arr,
            field,
            schema,
            _marker: std::marker::PhantomData,
        }
    }

    fn convert(self) -> Result<FieldArray, PolarsError> {
        let mut new_chunks = Vec::with_capacity(self.schema.len());

        match self.field.name.as_str() {
            "signal" => {
                let farr = minknow_vbz_to_large_binary(&self.field, vec![self.arr])?;
                new_chunks.push(farr.arr);
            }
            "read_id" => {
                let farr = minknow_uuid_to_fixed_size_binary(&self.field, vec![self.arr])?;
                new_chunks.push(farr.arr);
            }
            _ => {
                let farr = FieldArray::new(self.field.clone(), self.arr.clone());
                new_chunks.push(farr.arr);
            }
        }
        todo!()
    }
}

#[derive(Debug)]
pub(crate) struct FieldArray {
    pub(crate) field: pl::ArrowField,
    pub(crate) arr: Box<dyn Array>,
}

impl FieldArray {
    fn new(field: pl::ArrowField, arr: Box<dyn Array>) -> Self {
        Self { field, arr }
    }
}

pub(crate) fn field_arrs_to_schema(farrs: &[FieldArray]) -> Schema<pl::ArrowField> {
    farrs.iter().map(|farr| farr.field.clone()).collect()
}

pub(crate) fn field_arrs_to_record_batch(
    farrs: Vec<FieldArray>,
    schema: ArrowSchemaRef,
) -> RecordBatchT<Box<dyn Array>> {
    let height = farrs[0].arr.len();
    let arrays = farrs.into_iter().map(|f| f.arr).collect::<Vec<_>>();
    RecordBatchT::new(height, schema, arrays)
}

/// Converts a signal array into a LargeBinary array.
///
/// The signal can be a list[f32], list[i16], or list[u8](?) depending on
/// how it was processed.
fn minknow_vbz_to_large_binary(
    field: &pl::ArrowField,
    chunks: Vec<Box<dyn Array>>,
) -> Result<FieldArray, polars::error::PolarsError> {
    log::debug!("Converting signal minknow.vbz column");
    let new_dt = ArrowDataType::Extension(Box::new(ExtensionType {
        name: "minknow.vbz".into(),
        inner: ArrowDataType::LargeBinary,
        metadata: None,
    }));
    let new_field = ArrowField::new(field.name.clone(), new_dt.clone(), true);
    // let mut acc = MutableBinaryArray::<i64>::new();
    let mut acc: MutableBinaryArray<i64> =
        MutableBinaryArray::try_new(new_dt, Default::default(), Default::default(), None).unwrap();
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
    let acc = <BinaryArray<i64> as From<MutableBinaryArray<i64>>>::from(acc).boxed();
    Ok(FieldArray::new(new_field, acc))
}

/// Convert a read_id array into a FixedSizeBinary array.
///
/// The read_id is usually a str column and we try to convert from
/// different types of Utf8* arrays.
fn minknow_uuid_to_fixed_size_binary(
    field: &pl::ArrowField,
    chunks: Vec<Box<dyn Array>>,
) -> Result<FieldArray, polars::error::PolarsError> {
    let new_dt = ArrowDataType::Extension(Box::new(ExtensionType {
        name: "minknow.uuid".into(),
        inner: ArrowDataType::FixedSizeBinary(16),
        metadata: None,
    }));
    let new_field = ArrowField::new(field.name.clone(), new_dt.clone(), true);
    // let mut acc = MutableFixedSizeBinaryArray::new(16);
    let mut acc = MutableFixedSizeBinaryArray::try_new(new_dt, Vec::new(), None).unwrap();
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
    let acc = <FixedSizeBinaryArray as From<_>>::from(acc).boxed();
    log::debug!("new read_id {acc:?}");
    Ok(FieldArray::new(new_field, acc))
}

/// Main entrypoint for series conversion. By default it converts the arrays as
/// is, but for columns that are converted by array_to_series, we need to
/// convert back to the original type used in POD5 tables.
// pub(crate) fn series_to_array(series: Series) -> FieldArray {
//     let name = series.name().clone();
//     let field = series
//         .dtype()
//         .to_arrow_field(name.clone(), CompatLevel::newest());
//     log::debug!("series_to_array: {field:?}");
//     let chunks = series.into_chunks();
//     log::debug!("example chunk: {:?}", &chunks[0]);
//     match (field.name.as_str(), &field.dtype) {
//         ("signal", _) => minknow_vbz_to_large_binary(&field, chunks).unwrap(),
//         ("read_id", _) => minknow_uuid_to_fixed_size_binary(&field, chunks).unwrap(),
//         _ => {
//             let chunks = chunks.iter().map(|b| b.as_ref()).collect::<Vec<_>>();
//             let acc = concatenate::concatenate(&chunks).unwrap();
//             let farr = FieldArray::new(field, acc);
//             log::debug!("series_to_array: {farr:?}");
//             farr
//         }
//     }
// }

#[derive(Debug, thiserror::Error)]
pub enum CompatError {
    #[error("Error handing read_id column: {0}")]
    MinknowUuid(PolarsError),

    #[error("Error handing signal column: {0}")]
    MinknowVbz(PolarsError),

    #[error("Error converting field {0}, {1}")]
    GeneralConversionError(String, PolarsError),

    #[error("Error converting Large List to List, field {0}")]
    LargeToSmallList(String),
}

pub(crate) fn convert_by(arr: Box<dyn Array>, dt: ArrowDataType) -> Box<dyn Array> {
    todo!()
}

pub(crate) fn large_list_to_small_list(
    field: &ArrowField,
    arr: Box<dyn Array>,
) -> Result<FieldArray, polars::error::PolarsError> {
    let new_dt = ListArray::<i32>::default_datatype(ArrowDataType::UInt64);
    let new_field = ArrowField::new(field.name.clone(), new_dt.clone(), true);
    let mut acc = MutableListArray::<i32, MutablePrimitiveArray<u64>>::new();
    arr.as_any()
        .downcast_ref::<ListArray<i64>>()
        .unwrap()
        .iter()
        .for_each(|s| {
            let s = s.map(|inner| {
                inner
                    .as_any()
                    .downcast_ref::<PrimitiveArray<u64>>()
                    .unwrap()
                    .clone()
            });
            let s: Option<Vec<Option<u64>>> =
                s.map(|inner| inner.iter().map(|opt| opt.cloned()).collect());
            acc.try_push(s).unwrap();
        });
    let acc = <ListArray<i32> as From<_>>::from(acc);
    Ok(FieldArray::new(new_field, acc.boxed()))
}

pub(crate) fn record_batch_to_compat(
    batch: RecordBatchT<Box<dyn Array>>,
) -> Result<RecordBatchT<Box<dyn Array>>, CompatError> {
    let height = batch.height();
    let mut new_schema = Schema::with_capacity(batch.schema().len());
    let mut new_chunks = Vec::with_capacity(batch.arrays().len());

    let (schema, chunks) = batch.into_schema_and_arrays();

    for ((name, field), array) in schema.iter().zip(chunks.into_iter()) {
        log::debug!("record_btach_to_compat, field {field:?}");
        let farr = match (name.as_str(), &field.dtype) {
            // Signal in the ReadTable (list(u64))
            ("signal", ArrowDataType::LargeList(x))
                if matches!(x.dtype(), ArrowDataType::UInt64) =>
            {
                large_list_to_small_list(field, array)
                    .map_err(|_| CompatError::LargeToSmallList(name.to_string()))?
                // minknow_vbz_to_large_binary(field,
                // vec![array]).map_err(CompatError::MinknowVbz)?
            }
            // Signal in the SignalTable (list[i16] or largebinary)
            ("signal", _) => {
                minknow_vbz_to_large_binary(field, vec![array]).map_err(CompatError::MinknowVbz)?
            }
            ("read_id", _) => minknow_uuid_to_fixed_size_binary(field, vec![array])
                .map_err(CompatError::MinknowUuid)?,
            (name, ArrowDataType::Utf8View) => utf8view_to_utf8(field, vec![array])
                .map_err(|e| CompatError::GeneralConversionError(name.to_string(), e))?,
            (name @ ("context_tags" | "tracking_id"), _) => {
                context_tags_to_map(name, field, vec![array])
                    .map_err(|e| CompatError::GeneralConversionError(name.to_string(), e))?
            }
            _ => FieldArray::new(field.clone(), array),
        };
        new_schema.insert(name.clone(), farr.field);
        new_chunks.push(farr.arr);
    }
    Ok(RecordBatchT::new(height, Arc::new(new_schema), new_chunks))
}

fn convert_map_struct_arr(struct_arr: StructArray) -> StructArray {
    let (fields, length, data, validity) = struct_arr.clone().into_data();
    let data = data
        .into_iter()
        .map(|datum| {
            let mut utf8_arr: MutableUtf8Array<i32> = MutableUtf8Array::new();
            datum
                .as_any()
                .downcast_ref::<Utf8ViewArray>()
                .unwrap()
                .iter()
                .for_each(|s| utf8_arr.push(s));
            <Utf8Array<i32> as From<MutableUtf8Array<i32>>>::from(utf8_arr).boxed()
        })
        .collect::<Vec<_>>();

    let fields = vec![
        ArrowField::new("key".into(), ArrowDataType::Utf8, false),
        ArrowField::new("value".into(), ArrowDataType::Utf8, false),
    ];

    let new_dt = ArrowDataType::Struct(fields);
    StructArray::new(new_dt, length, data, validity)
}

fn context_tags_to_map(
    name: &str,
    field: &ArrowField,
    chunks: Vec<Box<dyn Array>>,
) -> Result<FieldArray, polars::error::PolarsError> {
    log::debug!("Converting context tags");
    log::debug!("Field: {field:?}");
    log::debug!("chunks: {chunks:?}");
    let new_field: ArrowField = map_field(name).1;
    log::debug!("New field: {new_field:?}");
    let list_arr = chunks.into_iter().next().unwrap();
    let list_arr = list_arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
    let offsets = OffsetsBuffer::try_from(list_arr.offsets()).unwrap();
    let validity = list_arr.validity();
    let chunk = list_arr.into_iter().next().unwrap().unwrap();
    // let mut key = name_field("key", ArrowDataType::Utf8).1;
    // let value = name_field("value", ArrowDataType::Utf8).1;
    // key.is_nullable = false;
    let fields = vec![
        ArrowField::new("key".into(), ArrowDataType::Utf8, false),
        ArrowField::new("value".into(), ArrowDataType::Utf8, false),
    ];
    let dt = ArrowDataType::Map(
        Box::new(ArrowField {
            name: "entries".into(),
            dtype: ArrowDataType::Struct(fields),
            is_nullable: false,
            metadata: None,
        }),
        false,
    );
    let struct_arr = chunk.as_any().downcast_ref::<StructArray>().unwrap();
    let struct_arr = convert_map_struct_arr(struct_arr.clone());
    // for kv in struct_arr.values() {
    //     log::debug!("origin arr {:?}", kv);
    //     let mut utf8_arr: MutableUtf8Array<i32> = MutableUtf8Array::new();
    //     kv.as_any()
    //         .downcast_ref::<Utf8ViewArray>()
    //         .unwrap()
    //         .iter()
    //         .for_each(|s| utf8_arr.push(s));
    //     log::debug!(
    //         "utf8 arr {:?}",
    //         <Utf8Array<i32> as From<MutableUtf8Array<i32>>>::from(utf8_arr)
    //     )
    //     // log::debug!("k {:?}", kv[0]);
    //     // log::debug!("kdt {:?}", kv[0].dtype());
    //     // log::debug!("karr {:?}",
    //     // kv[0].as_any().downcast_ref::<BinaryViewScalar<str>>().unwrap());
    //     // log::debug!("v {:?}", kv[1]);
    //     // log::debug!("vdt {:?}", kv[1].dtype());
    //     // log::debug!("varr {:?}",
    //     // kv[1].as_any().downcast_ref::<BinaryViewScalar<str>>().unwrap());
    // }

    log::debug!("field dtype {:?}", chunk.dtype());
    log::debug!("dt {:?}", dt);
    log::debug!("new struct {:?}", struct_arr.dtype());

    let map_array = MapArray::new(dt, offsets, struct_arr.boxed(), validity.cloned());
    // chunks.into_iter().for_each(|chunk| {
    //     for struct_arr in
    // chunk.as_any().downcast_ref::<ListArray<i64>>().unwrap() {         let
    // struct_arr = struct_arr.unwrap();         let struct_arr =
    // struct_arr.as_any().downcast_ref::<StructArray>().unwrap();         for
    // pair in struct_arr.iter() {             if pair.is_some() {
    //                 offset += 1;
    //             }
    //             offsets.push(offset);
    //         }
    //         log::debug!("{struct_arr:?}");
    //     }
    //     todo!()
    // });
    // panic!();
    Ok(FieldArray {
        field: new_field,
        arr: map_array.to_boxed(),
    })
}

fn utf8view_to_utf8(
    field: &ArrowField,
    chunks: Vec<Box<dyn Array>>,
) -> Result<FieldArray, polars::error::PolarsError> {
    log::debug!("Converting acquisition id");
    let new_dt = ArrowDataType::Utf8;
    let new_field = ArrowField::new(field.name.clone(), new_dt, true);
    let mut acc: MutableUtf8Array<i32> = MutableUtf8Array::new();
    chunks.into_iter().for_each(|chunk| match &field.dtype {
        ArrowDataType::Utf8View => {
            chunk
                .as_any()
                .downcast_ref::<Utf8ViewArray>()
                .unwrap()
                .values_iter()
                .map(Some)
                .for_each(|item| acc.push(item));
        }
        _ => todo!(),
    });
    log::debug!("New field: {new_field:?}");
    let acc = acc.as_box();
    Ok(FieldArray::new(new_field, acc))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use polars::{df, prelude::NamedFrom};
    use polars_arrow::{
        array::{BooleanArray, Int32Array, MapArray, StructArray},
        io::ipc::write::{FileWriter as ArrowFileWriter, WriteOptions},
        offset::OffsetsBuffer,
    };

    use super::*;
    use crate::dataframe::{AdcData, Calibration, SignalDataFrame};

    fn init() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();
    }

    #[test]
    fn test_maparray() {
        let boolean = BooleanArray::from_slice([false, false, true, true]).boxed();
        let int = Int32Array::from_slice([42, 28, 19, 31]).boxed();

        let fields = vec![
            ArrowField::new("b".into(), ArrowDataType::Boolean, false),
            ArrowField::new("c".into(), ArrowDataType::Int32, false),
        ];

        let array = StructArray::new(
            ArrowDataType::Struct(fields.clone()),
            4,
            vec![boolean, int],
            None,
        );

        let offsets = vec![0, 1, 2, 3, 4];
        let dt = ArrowDataType::Map(
            Box::new(ArrowField {
                name: "entries".into(),
                dtype: ArrowDataType::Struct(fields),
                is_nullable: false,
                metadata: None,
            }),
            false,
        );

        // Construct the MapArray
        let map_array = MapArray::try_new(
            dt,
            OffsetsBuffer::try_from(offsets).unwrap(),
            array.to_boxed(),
            None,
        )
        .unwrap();

        // Now you can use map_array in your application
        println!("{:?}", map_array);
    }

    // #[test]
    // fn test_series_to_array_minknow_uuid() {
    //     init();
    //     let example = String::from("67e55044-10b1-426f-9247-bb680e5fe0c8");
    //     let series = Series::new("read_id".into(), [example]);
    //     let farr = series_to_array(series);
    // }

    // #[test]
    // fn test_series_to_array_minknow_vbz() {
    //     init();
    //     let example = Some(b"test" as &[u8]);
    //     let series = Series::new("signal".into(), [example]);
    //     let farr = series_to_array(series);
    // }

    // #[test]
    // fn test_series_to_array_samples() {
    //     init();
    //     let example = [Some(717u32), Some(123u32)];
    //     let series = Series::new("samples".into(), example);
    //     let farr = series_to_array(series);
    // }

    // #[test]
    // fn test_writer() {
    //     init();
    //     let example = Some(b"test" as &[u8]);
    //     let series = Series::new("signal".into(), [example]);
    //     let farr = vec![series_to_array(series)];
    //     let schema = Arc::new(field_arrs_to_schema(&farr));
    //     let chunk = field_arrs_to_record_batch(farr, schema.clone());

    //     let buf: Vec<u8> = Vec::new();
    //     let mut writer =
    //         ArrowFileWriter::try_new(buf, schema.clone(), None,
    // WriteOptions::default()).unwrap();     writer.write(&chunk,
    // None).unwrap();     writer.finish().unwrap();
    // }

    // #[test]
    // fn test_df() {
    //     init();
    //     let cal = Calibration(
    //         [(
    //             "67e55044-10b1-426f-9247-bb680e5fe0c8".into(),
    //             AdcData {
    //                 offset: 0.0,
    //                 scale: 1.0,
    //             },
    //         )]
    //         .into_iter()
    //         .collect(),
    //     );
    //     let df = SignalDataFrame(df!("read_id" =>
    // ["67e55044-10b1-426f-9247-bb680e5fe0c8",
    // "67e55044-10b1-426f-9247-bb680e5fe0c8"],
    // "signal" => [[0.1f32, 0.2f32].iter().collect::<Series>(), [0.3f32,
    // 0.4f32].iter().collect::<Series>()],
    // "samples" => [2u32, 2u32],
    // ).unwrap());     println!("{df:?}");
    //     let df = df.with_adc(&cal);
    //     println!("{df:?}");
    //     let field_arrays =
    //         df.0.iter()
    //             .map(|s| series_to_array(s.clone()))
    //             .collect::<Vec<_>>();
    //     let schema = Arc::new(field_arrs_to_schema(&field_arrays));
    //     let chunk = field_arrs_to_record_batch(field_arrays, schema.clone());
    //     let buf: Vec<u8> = Vec::new();
    //     let mut writer =
    //         ArrowFileWriter::try_new(buf, schema.clone(), None,
    // WriteOptions::default()).unwrap();     writer.write(&chunk,
    // None).unwrap();     writer.finish().unwrap();
    // }
}
