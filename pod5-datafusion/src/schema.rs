use std::{any::Any, io::SeekFrom, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        buffer::Buffer,
        ipc::{
            convert::fb_to_schema,
            reader::{FileDecoder, read_footer_length},
            root_as_footer,
        },
    },
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::Result,
};
use pod5_format::footer_generated::minknow::reads_format::ContentType;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::Mutex,
};

use crate::{
    footer::{self, ParsedFooter},
    table::Pod5TableProvider,
};

fn table_name_to_content_type(name: &str) -> Option<ContentType> {
    match name {
        "signal" => Some(ContentType::SignalTable),
        "run_info" => Some(ContentType::RunInfoTable),
        "reads" => Some(ContentType::ReadsTable),
        "read_id" => Some(ContentType::ReadIdIndex),
        _ => None,
    }
}

#[derive(Debug)]
pub struct Pod5SchemaProvider {
    pod5_file: Mutex<File>,
    footer: ParsedFooter,
}

impl Pod5SchemaProvider {
    pub async fn new(mut file: File) -> Self {
        let footer = footer::ParsedFooter::read_footer(&mut file).await.unwrap();
        Self {
            pod5_file: Mutex::new(file),
            footer,
        }
    }

    pub async fn create(file: File) -> Arc<Self> {
        Arc::new(Self::new(file).await)
    }
}

#[async_trait]
impl SchemaProvider for Pod5SchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        ["signal", "run_info", "reads", "read_id"]
            .into_iter()
            .filter(|name| self.table_exist(name))
            .map(|name| name.to_owned())
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let Some(ct) = table_name_to_content_type(name) else {
            return Err(datafusion::error::DataFusionError::Plan(
                "Table not found".to_owned(),
            ));
        };

        let table_info = self.footer.find_table(ct).unwrap();
        let length = table_info.length() as u64;
        let offset = table_info.offset() as u64;
        let mut buf = vec![0u8; length as usize];

        {
            let mut file = self.pod5_file.lock().await;
            file.seek(SeekFrom::Start(offset)).await?;
            file.read_exact(&mut buf).await?;
        }

        let buffer = Buffer::from(buf);

        let trailer_start = buffer.len() - 10;
        let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap()).unwrap();
        let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start]).unwrap();

        let schema = fb_to_schema(footer.schema().unwrap());
        let schema = Arc::new(schema);
        let decoder = FileDecoder::new(schema.clone(), footer.version());
        let batches = footer.recordBatches().unwrap();
        let rbs: Vec<_> = (0..batches.len())
            .map(|idx| {
                let block = batches.get(idx);
                let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
                let data = buffer.slice_with_length(block.offset() as _, block_len);
                let batch = decoder.read_record_batch(block, &data).unwrap().unwrap();
                batch
            })
            .collect();
        Ok(Some(Arc::new(Pod5TableProvider::new(schema.clone(), rbs)?)))
    }

    fn table_exist(&self, name: &str) -> bool {
        table_name_to_content_type(name)
            .and_then(|ct| self.footer.find_table(ct).ok())
            .is_some()
    }
}
