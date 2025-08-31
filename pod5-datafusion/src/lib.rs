use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::Schema,
    catalog::Session,
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileSource},
    },
    error::DataFusionError,
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

#[derive(Debug)]
pub struct Pod5FileFormat;

impl FileFormat for Pod5FileFormat {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn get_ext(&self) -> String {
        todo!()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String, DataFusionError> {
        todo!()
    }

    #[doc = " Returns whether this instance uses compression if applicable"]
    fn compression_type(&self) -> Option<FileCompressionType> {
        todo!()
    }

    fn infer_schema<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        store: &'life2 Arc<dyn ObjectStore>,
        objects: &'life3 [ObjectMeta],
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<Arc<Schema>, DataFusionError>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    fn infer_stats<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        store: &'life2 Arc<dyn ObjectStore>,
        table_schema: Arc<Schema>,
        object: &'life3 ObjectMeta,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<Statistics, DataFusionError>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    fn create_physical_plan<'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        conf: FileScanConfig,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = Result<Arc<dyn ExecutionPlan>, DataFusionError>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct Pod5FileFactory;

impl GetExt for Pod5FileFactory {
    fn get_ext(&self) -> String {
        todo!()
    }
}

impl FileFormatFactory for Pod5FileFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        todo!()
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        todo!()
    }
}
