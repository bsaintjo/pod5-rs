use std::sync::Arc;

use polars::prelude::{ArrowDataType, ArrowField};
use polars_arrow::datatypes::{ArrowSchema, ArrowSchemaRef};

use super::{dictionary_field, name_field, signal_schema::read_id};

#[derive(Debug, Clone)]
pub struct ReadsSchema {
    pub(crate) inner: ArrowSchemaRef,
}


impl ReadsSchema {
    pub fn new() -> Self {
        let inner = Arc::new(ArrowSchema::from_iter([
            read_id(),
            name_field("signal", ArrowDataType::List(
                Box::new(ArrowField {
                    name: "item".into(),
                    dtype: ArrowDataType::UInt64,
                    is_nullable: true,
                    metadata: None,
                }),
            )),
            name_field("read_number", ArrowDataType::UInt32),
            name_field("start", ArrowDataType::UInt64),
            name_field("median_before", ArrowDataType::Float32),
            name_field("num_minknow_events", ArrowDataType::UInt64),
            name_field("tracked_scaling_scale", ArrowDataType::Float32),
            name_field("tracked_scaling_shift", ArrowDataType::Float32),
            name_field("predicted_scaling_scale", ArrowDataType::Float32),
            name_field("predicted_scaling_shift", ArrowDataType::Float32),
            name_field("num_reads_since_mux_change", ArrowDataType::UInt32),
            name_field("time_since_mux_change", ArrowDataType::Float32),
            name_field("num_samples", ArrowDataType::UInt64),
            name_field("channel", ArrowDataType::UInt16),
            name_field("well", ArrowDataType::UInt8),
            dictionary_field("pore_type"),
            name_field("calibration_offset", ArrowDataType::Float32),
            name_field("calibration_scale", ArrowDataType::Float32),
            dictionary_field("end_reason"),
            name_field("end_reason_forced", ArrowDataType::Boolean),
            dictionary_field("run_info"),
        ]));
        Self { inner }
    }

    pub fn inner(&self) -> &ArrowSchemaRef {
        &self.inner
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
        let signal_df_iter = reader.read_dfs().unwrap();
        pretty_assertions::assert_eq!(
            **ReadsSchema::new().inner(),
            signal_df_iter.table_reader.schema().clone()
        );
    }

}
