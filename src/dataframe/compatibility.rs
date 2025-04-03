use polars::{
    datatypes::ArrowDataType,
    prelude::{ArrowField, CompatLevel, LargeBinaryArray, PlSmallStr},
    series::Series,
};

use polars::prelude as pl;
use polars_arrow::{
    array::{Array, FixedSizeBinaryArray, Utf8Array, Utf8ViewArray},
    datatypes::ExtensionType,
};
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
    let field = series
        .dtype()
        .to_arrow_field(name.clone(), CompatLevel::newest());
    let chunks = series.into_chunks();
    match (field.name.as_str(), &field.dtype) {
        ("minknow.vbz", _) => {
            let new_dt = ArrowDataType::Extension(Box::new(ExtensionType {
                name: name.clone(),
                inner: ArrowDataType::LargeBinary,
                metadata: None,
            }));
            let new_field = ArrowField::new(name.clone(), new_dt, true);
            todo!()
        }
        ("minknow.uuid", _) => {
            let new_dt = ArrowDataType::Extension(Box::new(ExtensionType {
                name: name.clone(),
                inner: ArrowDataType::FixedSizeBinary(16),
                metadata: None,
            }));
            let new_field = ArrowField::new(name.clone(), new_dt, true);
            let chunks = chunks
                .into_iter()
                .map(|chunk| {
                    let arr = match &field.dtype {
                        ArrowDataType::Utf8View => chunk
                            .as_any()
                            .downcast_ref::<Utf8ViewArray>()
                            .unwrap()
                            .values_iter()
                            .map(|s| {
                                Some(Vec::from(s.parse::<Uuid>().unwrap()).try_into().unwrap())
                            })
                            .collect::<Vec<Option<[u8; 16]>>>(),
                        _ => chunk
                            .as_any()
                            .downcast_ref::<Utf8Array<i32>>()
                            .unwrap()
                            .values_iter()
                            .map(|s| {
                                Some(Vec::from(s.parse::<Uuid>().unwrap()).try_into().unwrap())
                            })
                            .collect::<Vec<Option<[u8; 16]>>>(),
                    };
                    FixedSizeBinaryArray::from(&arr).boxed()
                })
                .collect::<Vec<_>>();
            FieldArray::new(new_field, chunks)
        }
        _ => FieldArray::new(field, chunks),
    }
}

#[cfg(test)]
mod test {
    use polars::prelude::NamedFrom;

    use super::*;

    #[test]
    fn test_series_to_array_minknow_uuid() {
        let example = String::from("67e55044-10b1-426f-9247-bb680e5fe0c8");
        let series = Series::new("minknow.uuid".into(), [example]);
        let farr = series_to_array(series);
    }

    #[test]
    fn test_series_to_array_minknow_vbz() {
        let example = Some(b"test" as &[u8]);
        let series = Series::new("minknow.vbz".into(), [example]);
        println!("{:?}", series.to_arrow(0, CompatLevel::newest()));
        println!("{:?}", series.to_arrow(0, CompatLevel::oldest()));
        // let farr = series_to_array(series);
    }
}
