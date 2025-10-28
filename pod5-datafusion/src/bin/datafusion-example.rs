use std::{io::SeekFrom, path::Path, sync::Arc};

use datafusion::{
    arrow::{
        buffer::Buffer,
        ipc::{
            convert::fb_to_schema,
            reader::{FileDecoder, read_footer_length},
            root_as_footer,
        },
    },
    error::Result,
    prelude::SessionContext,
};
use pod5_datafusion::footer;
use pod5_format::footer_generated::minknow::reads_format::ContentType;
use tokio::{
    fs::{File, try_exists},
    io::{AsyncReadExt, AsyncSeekExt},
};

#[tokio::main]
async fn main() -> Result<()> {
    let filepath = Path::new("extra/multi_fast5_zip_v3.pod5");
    assert!(try_exists(&filepath).await?);
    let mut file = File::open(filepath).await?;
    let footer = footer::ParsedFooter::read_footer(&mut file).await.unwrap();
    println!("footer: {:?}", footer.footer());

    let table_info = footer.find_table(ContentType::RunInfoTable).unwrap();
    let length = table_info.length() as u64;
    let offset = table_info.offset() as u64;
    let mut buf = vec![0u8; length as usize];

    file.seek(SeekFrom::Start(offset)).await?;
    file.read_exact(&mut buf).await?;

    let buffer = Buffer::from(buf);

    let trailer_start = buffer.len() - 10;
    let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap()).unwrap();
    let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start]).unwrap();

    let schema = fb_to_schema(footer.schema().unwrap());
    let schema = Arc::new(schema);

    let decoder = FileDecoder::new(schema.clone(), footer.version());
    println!("{decoder:?}");

    let batches = footer.recordBatches().unwrap();
    println!("{}", batches.len());

    let block = batches.get(0);
    let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
    let data = buffer.slice_with_length(block.offset() as _, block_len);
    let batch = decoder.read_record_batch(block, &data).unwrap().unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("signal", batch).unwrap();
    let res = ctx
        .sql("select * from signal limit 5")
        .await?
        .collect()
        .await?;
    println!("{res:?}");
    Ok(())
}
