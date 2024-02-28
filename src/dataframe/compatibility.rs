use polars::datatypes::ArrowDataType;

use polars::prelude as pl;
use polars_arrow::{
    array::{
        growable::{Growable, GrowableList},
        Array, BinaryArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray, ListArray,
        MapArray, NullArray, PrimitiveArray, StructArray, Utf8Array,
    },
    bitmap::{Bitmap, MutableBitmap},
    compute::{
        cast::{
            binary_large_to_binary, fixed_size_binary_binary, primitive_to_primitive,
            utf8_to_large_utf8,
        },
        take::take_unchecked,
    },
    types::{Index, Offset},
};
// Copied from polars_arrow because it isn't exported
///
/// Relevant code: https://docs.rs/polars-arrow/0.37.0/src/polars_arrow/compute/take/structure.rs.html#23
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
fn convert_dictionaries(arr: Box<dyn Array>) -> Box<dyn Array> {
    let arr_dict = arr.as_any().downcast_ref::<DictionaryArray<i16>>().unwrap();
    let pl::ArrowDataType::Struct(..) = arr_dict.values().data_type() else {
        return arr;
    };
    let indices = primitive_to_primitive::<_, i64>(arr_dict.keys(), &pl::ArrowDataType::Int64);
    let s = arr_dict
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let pl::ArrowDataType::Struct(fields) = s.data_type().clone() else {
        unreachable!()
    };
    let mut new_fields = Vec::with_capacity(fields.capacity());
    let brr: Vec<Box<dyn Array>> = s
        .values()
        .iter()
        .zip(fields)
        .map(|(a, mut f)| {
            let b = if a.data_type() == &pl::ArrowDataType::Utf8 {
                let conc = a.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
                let res = utf8_to_large_utf8(conc);
                f.data_type = ArrowDataType::LargeUtf8;
                res.boxed()
            } else {
                a.to_boxed()
            };

            if let pl::ArrowDataType::Map(..) = b.data_type() {
                let marr = b.as_any().downcast_ref::<MapArray>().unwrap();
                let inner = marr.field().clone();
                let ldt = ListArray::<i32>::default_datatype(inner.data_type().clone());
                let lres = ListArray::<i32>::new(
                    ldt,
                    marr.offsets().clone(),
                    inner,
                    marr.validity().cloned(),
                );
                f.data_type = lres.data_type().clone();
                new_fields.push(f);
                unsafe { take_unchecked_list(&lres, &indices).to_boxed() }
            } else {
                new_fields.push(f);
                unsafe { take_unchecked(b.as_ref(), &indices) }
            }
        })
        .collect();
    let validity = unsafe { take_validity(s.validity(), &indices) };
    StructArray::new(pl::ArrowDataType::Struct(new_fields), brr, validity).boxed()
}

unsafe fn take_unchecked_list<I: Offset, O: Index>(
    values: &ListArray<I>,
    indices: &PrimitiveArray<O>,
) -> ListArray<I> {
    let mut capacity = 0;
    let arrays = indices
        .values()
        .iter()
        .map(|index| {
            let index = index.to_usize();
            let slice = values.clone().sliced(index, 1);
            capacity += slice.len();
            slice
        })
        .collect::<Vec<ListArray<I>>>();

    let arrays = arrays.iter().collect();

    if let Some(validity) = indices.validity() {
        let mut growable: GrowableList<I> = GrowableList::new(arrays, true, capacity);

        for index in 0..indices.len() {
            if validity.get_bit_unchecked(index) {
                growable.extend(index, 0, 1);
            } else {
                growable.extend_validity(1)
            }
        }

        growable.into()
    } else {
        let mut growable: GrowableList<I> = GrowableList::new(arrays, false, capacity);
        for index in 0..indices.len() {
            growable.extend(index, 0, 1);
        }

        growable.into()
    }
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
        // ArrowDataType::Timestamp(_, ) => todo!(),
        // ArrowDataType::Date32 => todo!(),
        pl::ArrowDataType::Date64 => todo!(),
        // ArrowDataType::Time32(_) => todo!(),
        // ArrowDataType::Time64(_) => todo!(),
        // ArrowDataType::Duration(_) => todo!(),
        pl::ArrowDataType::Interval(_) => todo!("{dt:?}"),
        // ArrowDataType::Binary => todo!("{dt:?}"),
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
        _ => arr.to_boxed(),
    }
}
