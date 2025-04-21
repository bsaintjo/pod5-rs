use std::sync::Arc;

use polars::prelude::{ArrowDataType, ArrowSchema, ArrowTimeUnit};
use polars_arrow::datatypes::ArrowSchemaRef;

use super::{map_field, name_field};

#[derive(Debug, Clone)]
pub struct RunInfoSchema {
    inner: ArrowSchemaRef,
}

fn timestamp_dt() -> ArrowDataType {
    ArrowDataType::Timestamp(ArrowTimeUnit::Millisecond, Some("UTC".into()))
}

impl RunInfoSchema {
    pub fn new() -> Self {
        let inner = Arc::new(ArrowSchema::from_iter([
            name_field("acquisition_id", ArrowDataType::Utf8),
            name_field("acquisition_start_time", timestamp_dt()),
            name_field("adc_max", ArrowDataType::Int16),
            name_field("adc_min", ArrowDataType::Int16),
            map_field("context_tags"),
            name_field("experiment_name", ArrowDataType::Utf8),
            name_field("flow_cell_id", ArrowDataType::Utf8),
            name_field("flow_cell_product_code", ArrowDataType::Utf8),
            name_field("protocol_name", ArrowDataType::Utf8),
            name_field("protocol_run_id", ArrowDataType::Utf8),
            name_field("protocol_start_time", timestamp_dt()),
            name_field("sample_id", ArrowDataType::Utf8),
            name_field("sample_rate", ArrowDataType::UInt16),
            name_field("sequencing_kit", ArrowDataType::Utf8),
            name_field("sequencer_position", ArrowDataType::Utf8),
            name_field("sequencer_position_type", ArrowDataType::Utf8),
            name_field("software", ArrowDataType::Utf8),
            name_field("system_name", ArrowDataType::Utf8),
            name_field("system_type", ArrowDataType::Utf8),
            map_field("tracking_id"),
        ]));
        Self { inner }
    }

    pub fn into_inner(self) -> ArrowSchemaRef {
        self.inner
    }
}

#[cfg(test)]
mod test_super {
    use std::fs::File;

    use crate::reader::Reader;

    use super::*;

    #[test]
    fn test_run_info_schema() {
        let path = "extra/multi_fast5_zip_v3.pod5";
        let mut file = File::open(path).unwrap();
        let mut reader = Reader::from_reader(&mut file).unwrap();
        let signal_df_iter = reader.run_info_dfs().unwrap();
        pretty_assertions::assert_eq!(
            *RunInfoSchema::new().into_inner(),
            signal_df_iter.table_reader.schema().clone()
        );
    }

}
