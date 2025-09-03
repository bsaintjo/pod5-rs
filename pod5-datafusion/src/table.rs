use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{Schema, SchemaRef},
    },
    catalog::{
        MemTable, Session,
    },
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

#[derive(Debug)]
pub struct Pod5TableProvider {
    table: MemTable,
}

impl Pod5TableProvider {
    pub fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Result<Self, DataFusionError> {
        let table = MemTable::try_new(schema, vec![batches])?;
        Ok(Self { table })
    }
}

#[async_trait]
impl TableProvider for Pod5TableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn table_type(&self) -> TableType {
        self.table.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.table.scan(state, projection, filters, limit).await
    }
}
