use std::{any::Any, sync::Arc};

use datafusion::catalog::{CatalogProvider, SchemaProvider};

#[derive(Debug)]
pub struct Pod5CatalogProvider;

impl CatalogProvider for Pod5CatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![
            "signal".to_owned(),
            "reads".to_owned(),
            "run_info".to_owned(),
        ]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            "signal" => todo!(),
            "reads" => todo!(),
            "run_info" => todo!(),
            _ => None,
        }
    }
}
