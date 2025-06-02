use std::sync::Arc;

use polars::prelude::{ArrowDataType, ArrowField, PlSmallStr};
use polars_arrow::datatypes::{ArrowSchemaRef, IntegerType, Metadata};

pub mod reads_schema;
pub mod run_info_schema;
pub mod signal_schema;

pub trait TableSchema {
    fn as_schema() -> ArrowSchemaRef;
}

fn name_field_md<S, I, K, V>(name: S, dtype: ArrowDataType, metadata: I) -> (PlSmallStr, ArrowField)
where
    S: Into<PlSmallStr>,
    I: Iterator<Item = (K, V)>,
    K: Into<PlSmallStr>,
    V: Into<PlSmallStr>,
{
    let name = name.into();
    (
        name.clone(),
        ArrowField {
            name,
            dtype,
            is_nullable: true,
            metadata: Some(Arc::new(Metadata::from_iter(
                metadata.map(|(k, v)| (k.into(), v.into())),
            ))),
        },
    )
}

pub(crate) fn name_field<S: Into<PlSmallStr>>(
    name: S,
    dtype: ArrowDataType,
) -> (PlSmallStr, ArrowField) {
    let name = name.into();
    (
        name.clone(),
        ArrowField {
            name,
            dtype,
            is_nullable: true,
            metadata: Default::default(),
        },
    )
}

fn dictionary_field<S: Into<PlSmallStr>>(name: S) -> (PlSmallStr, ArrowField) {
    name_field(
        name,
        ArrowDataType::Dictionary(IntegerType::Int16, Box::new(ArrowDataType::Utf8), false),
    )
}

pub(crate) fn map_field<S: Into<PlSmallStr>>(name: S) -> (PlSmallStr, ArrowField) {
    let mut key = name_field("key", ArrowDataType::Utf8).1;
    let value = name_field("value", ArrowDataType::Utf8).1;
    key.is_nullable = false;
    name_field(
        name,
        ArrowDataType::Map(
            Box::new(ArrowField {
                name: "entries".into(),
                dtype: ArrowDataType::Struct(vec![key, value]),
                is_nullable: false,
                metadata: None,
            }),
            false,
        ),
    )
}
