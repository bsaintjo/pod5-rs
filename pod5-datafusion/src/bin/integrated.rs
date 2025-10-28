use std::{path::Path, sync::Arc};

use datafusion::{
    arrow::util::pretty,
    catalog::{CatalogProvider, MemoryCatalogProvider, MemoryCatalogProviderList},
    error::Result,
    prelude::{SessionContext, col},
};
use pod5_datafusion::schema::Pod5SchemaProvider;
use tokio::fs::{File, try_exists};

#[tokio::main]
async fn main() -> Result<()> {
    let filepath = Path::new("extra/multi_fast5_zip_v3.pod5");
    assert!(try_exists(&filepath).await?);
    let file = File::open(filepath).await?;

    let ctx = SessionContext::new();
    let catalog_list = Arc::new(MemoryCatalogProviderList::new());
    ctx.register_catalog_list(catalog_list.clone());

    let catalog = Arc::new(MemoryCatalogProvider::new());

    let schema = Pod5SchemaProvider::create(file).await;
    catalog.register_schema("first", schema.clone())?;

    ctx.register_catalog("catalog", catalog.clone());

    let result = ctx
        .sql("select * from catalog.first.run_info")
        .await?
        .select([
            col("acquisition_start_time"),
            col("protocol_name"),
            col("protocol_start_time"),
            col("software"),
        ])?
        .limit(0, Some(5))?
        .collect()
        .await;
    match result {
        Ok(batches) => {
            pretty::print_batches(&batches).unwrap();
        }
        Err(e) => {
            println!("Query failed: {e}");
        }
    }

    Ok(())
}
