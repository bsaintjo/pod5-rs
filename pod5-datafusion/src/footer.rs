use std::io::SeekFrom;

use flatbuffers::root;
use pod5_format::{
    FILE_SIGNATURE, TableInfo,
    footer_generated::minknow::reads_format::{ContentType, Footer},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::error::{FooterError, FormatError, Pod5Error};

#[derive(Debug)]
pub struct ParsedFooter {
    pub(crate) data: Vec<u8>,
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

    pub fn find_table(&self, content_type: ContentType) -> Result<TableInfo, FormatError> {
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
