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
use flatbuffers::root;
// use pod5_datafusion;
use pod5_format::{
    FILE_SIGNATURE, TableInfo,
    footer_generated::minknow::reads_format::{ContentType, Footer},
};
use tokio::{
    fs::{File, try_exists},
    io::{AsyncReadExt, AsyncSeekExt},
};

#[derive(Debug)]
pub struct FormatError;

#[derive(Debug)]
pub struct Pod5Error;

#[derive(Debug)]
pub struct FooterError;

pub struct ParsedFooter {
    data: Vec<u8>,
}

impl ParsedFooter {
    /// Parse a POD5 Flatbuffer footer from a reader containg data from a POD5
    /// file.
    pub async fn read_footer<R: AsyncReadExt + AsyncSeekExt + Unpin>(
        mut reader: R,
    ) -> Result<Self, Pod5Error> {
        reader.rewind().await.unwrap();
        let footer_length = -(FILE_SIGNATURE.len() as i64) + (-16) + (-8);
        reader.seek(SeekFrom::End(footer_length)).await.unwrap();
        let mut buf = [0; 8];
        reader.read_exact(&mut buf).await.unwrap();
        let flen = i64::from_le_bytes(buf);
        reader
            .seek(SeekFrom::End(footer_length - flen))
            .await
            .unwrap();
        let mut buf = vec![0u8; flen as usize];
        reader.read_exact(&mut buf).await.unwrap();
        Ok(Self { data: buf })
    }

    pub fn footer(&self) -> Result<Footer<'_>, FooterError> {
        Ok(root::<Footer>(&self.data).map_err(|_| FooterError)?)
    }

    fn find_table(&self, content_type: ContentType) -> Result<TableInfo, FormatError> {
        let footer = self.footer().unwrap();
        let contents = footer.contents().ok_or(FooterError).unwrap();
        let mut efile = None;
        for c in contents {
            if c.content_type() == content_type {
                efile = Some(c);
                break;
            }
        }
        let efile = efile.ok_or(FooterError).unwrap();

        Ok(TableInfo::new(efile.offset(), efile.length(), content_type))
    }
}

pub async fn valid_signature<R>(mut reader: R) -> bool
where
    R: AsyncReadExt + Unpin,
{
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await.unwrap();
    buf == FILE_SIGNATURE
}

#[tokio::main]
async fn main() -> Result<()> {
    let filepath = Path::new("extra/multi_fast5_zip_v3.pod5");
    assert!(try_exists(&filepath).await?);
    let mut file = File::open(filepath).await?;
    let footer = ParsedFooter::read_footer(&mut file).await.unwrap();
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

    let decoder = FileDecoder::new(Arc::new(schema), footer.version());
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
