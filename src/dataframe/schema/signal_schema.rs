use std::sync::Arc;

use polars::prelude::{ArrowField, PlSmallStr};
use polars_arrow::datatypes::{ArrowDataType, ArrowSchemaRef, ExtensionType};

use super::{name_field, name_field_md, TableSchema};

pub(crate) fn read_id() -> (PlSmallStr, ArrowField) {
    name_field_md(
        "read_id",
        minknow_uuid(),
        [
            ("ARROW:extension:metadata", ""),
            ("ARROW:extension:name", "minknow.uuid"),
        ]
        .into_iter(),
    )
}

fn minknow_uuid() -> ArrowDataType {
    ArrowDataType::Extension(Box::new(ExtensionType {
        name: "minknow.uuid".into(),
        inner: ArrowDataType::FixedSizeBinary(16),
        metadata: Some("".into()),
    }))
}

fn minknow_vbz() -> ArrowDataType {
    ArrowDataType::Extension(Box::new(ExtensionType {
        name: "minknow.vbz".into(),
        inner: ArrowDataType::LargeBinary,
        metadata: Some("".into()),
    }))
}

#[derive(Debug, Clone)]
pub struct SignalSchema {
    inner: ArrowSchemaRef,
}

impl TableSchema for SignalSchema {
    fn as_schema() -> ArrowSchemaRef {
        Self::new().inner
    }
}

impl SignalSchema {
    pub fn new() -> Self {
        let inner = Arc::new(polars_arrow::datatypes::ArrowSchema::from_iter([
            read_id(),
            name_field_md(
                "signal",
                minknow_vbz(),
                [
                    ("ARROW:extension:metadata", ""),
                    ("ARROW:extension:name", "minknow.vbz"),
                ]
                .into_iter(),
            ),
            name_field("samples", ArrowDataType::UInt32),
        ]));
        Self { inner }
    }

    pub fn into_inner(self) -> ArrowSchemaRef {
        self.inner
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use super::*;
    use crate::reader::Reader;

    #[test]
    fn test_signal_schema() {
        let path = "extra/multi_fast5_zip_v3.pod5";
        let mut file = File::open(path).unwrap();
        let mut reader = Reader::from_reader(&mut file).unwrap();
        let signal_df_iter = reader.signal_dfs().unwrap();
        pretty_assertions::assert_eq!(
            *SignalSchema::new().into_inner(),
            signal_df_iter.table_reader.schema().clone()
        );
    }
}
