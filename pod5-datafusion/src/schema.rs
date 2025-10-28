use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{catalog::SchemaProvider, datasource::TableProvider, error::Result};

#[derive(Debug)]
pub struct Pod5SchemaProvider;

#[async_trait]
impl SchemaProvider for Pod5SchemaProvider {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn table_names(&self) -> Vec<String> {
        todo!()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        todo!()
    }

    fn table_exist(&self, name: &str) -> bool {
        todo!()
    }
}
