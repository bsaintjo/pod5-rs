use polars::{
    datatypes::ArrowDataType,
    prelude::{CompatLevel, LargeBinaryArray, PlSmallStr},
    series::Series,
};

use polars::prelude as pl;
use polars_arrow::array::{Array, FixedSizeBinaryArray, Utf8Array};
use uuid::Uuid;

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

struct FieldArray {
    field: pl::ArrowField,
    arr: Vec<Box<dyn Array>>,
}

impl FieldArray {
    fn new(field: pl::ArrowField, arr: Vec<Box<dyn Array>>) -> Self {
        Self { field, arr }
    }
}

fn series_to_array(series: Series) -> FieldArray {
    let name = series.name().clone();
    let field = series.dtype().to_arrow_field(name, CompatLevel::newest());
    let chunks = series.into_chunks();
    match (field.name.as_str(), field.dtype) {
        ("minknow.vbz", _) => todo!(),
        ("minknow.uuid", _) => todo!(),
        _ => todo!()
    }
    // FieldArray::new(field, chunks)
    todo!()
}
