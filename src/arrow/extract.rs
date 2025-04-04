use std::marker::PhantomData;

use arrow::array::{
    ArrayAccessor, ArrayIter, FixedSizeBinaryArray, Float32Array, RecordBatch, UInt16Array,
    UInt32Array,
};

use super::record::ReadId;

/// Generic trait for converting from Arrow arrays into corresponding vectors
pub(crate) trait ArrowExtract: private::Sealed {
    /// The specific Arrow Array type to convert into
    type ArrowArrayType;
    fn convert(int: Option<<&Self::ArrowArrayType as ArrayAccessor>::Item>) -> Self
    where
        for<'a> &'a Self::ArrowArrayType: ArrayAccessor;

    fn extract<'b>(batch: &'b RecordBatch, name: &str) -> Vec<Self>
    where
        Self: Sized,
        for<'a> &'a Self::ArrowArrayType: ArrayAccessor,
        for<'a> <Self as ArrowExtract>::ArrowArrayType: 'a,
    {
        let read_id = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Self::ArrowArrayType>()
            .unwrap();

        ArrayIter::new(read_id)
            .map(|x| Self::convert(x))
            .collect::<Vec<_>>()
    }
}

impl ArrowExtract for ReadId {
    type ArrowArrayType = FixedSizeBinaryArray;

    fn convert(int: std::option::Option<<&FixedSizeBinaryArray as ArrayAccessor>::Item>) -> Self {
        ReadId::new(int.unwrap().to_vec())
    }
}

macro_rules! extract_primitive {
    ($native_type:ty, $arr_type:ty) => {
        impl ArrowExtract for $native_type {
            type ArrowArrayType = $arr_type;

            fn convert(int: std::option::Option<<&$arr_type as ArrayAccessor>::Item>) -> Self {
                int.unwrap()
            }
        }
    };
}

extract_primitive!(u32, UInt32Array);

extract_primitive!(u16, UInt16Array);

extract_primitive!(f32, Float32Array);

pub(crate) struct Dict<T> {
    pub(crate) phantom: PhantomData<T>,
}

pub(crate) mod private {
    use crate::arrow::record::ReadId;

    pub trait Sealed {}
    impl Sealed for ReadId {}
    impl Sealed for u32 {}
    impl Sealed for u16 {}
    impl Sealed for f32 {}
}
