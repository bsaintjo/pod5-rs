//! Traits and types for implementing POD5 IOs
//!
//! POD5 files are a container file around multiple Apache Arrow files. There
//! are multiple implementations (official Arrow, Polars, DataFusion, etc.) that
//! provide a way of interacting with Arrow files. This crate provides an
//! abstraction that handles all the container-related code, and simplify the
//! integration of alternative Arrow provides to working with POD5 files.
use std::{
    io::{Read, Seek, SeekFrom},
    marker::PhantomData,
};

use futures_lite::{AsyncReadExt, AsyncSeekExt};
use pod5_format::{FILE_SIGNATURE, FormatError, ParsedFooter, valid_signature};

#[derive(Debug, thiserror::Error)]
pub enum Pod5Error {
    #[error("Signature failure: {0}")]
    SignatureFailure(&'static str),
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Format Error: {0}")]
    FormatError(#[from] FormatError),
}

pub trait ArrowProvider {
    type Dataset;
    fn from_bytes(bs: &[u8]) -> Self::Dataset;
}

pub struct Reader<P: ArrowProvider, R> {
    reader: R,
    footer: ParsedFooter,
    _x: PhantomData<P>,
}

#[derive(Debug)]
pub struct SignalTable<P: ArrowProvider>(pub P::Dataset);

#[derive(Debug)]
pub struct ReadInfoTable<P: ArrowProvider>(pub P::Dataset);

#[derive(Debug)]
pub struct ReadsTable<P: ArrowProvider>(pub P::Dataset);

#[derive(Debug)]
pub struct OtherTable<P: ArrowProvider>(pub P::Dataset);

impl<P: ArrowProvider, R: Read + Seek> Reader<P, R> {
    pub fn from_reader(&self, mut reader: R) -> Result<Self, Pod5Error> {
        if !valid_signature(&mut reader)? {
            return Err(Pod5Error::SignatureFailure("Start"));
        }
        reader.seek(SeekFrom::End(-8))?;
        if !valid_signature(&mut reader)? {
            return Err(Pod5Error::SignatureFailure("End"));
        }
        let footer = ParsedFooter::read_footer(&mut reader)?;
        Ok(Self {
            reader,
            footer,
            _x: PhantomData,
        })
    }
    pub fn signal_table(&mut self) -> Result<SignalTable<P>, Pod5Error> {
        let table = self.footer.signal_table()?;
        let offset = table.as_ref().offset() as u64;
        let length = table.as_ref().length() as usize;

        let mut buf = vec![0u8; length];
        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.read_exact(&mut buf)?;
        let table = P::from_bytes(&buf);
        Ok(SignalTable(table))
    }
}

pub struct AsyncReader<R> {
    reader: R,
    footer: ParsedFooter,
}

impl<R: AsyncReadExt + AsyncSeekExt + Unpin> AsyncReader<R> {
    async fn from_reader(mut reader: R) -> Result<Self, std::io::Error> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf).await?;
        if !valid_signature_sansio(buf) {

        }
        reader.seek(SeekFrom::End(-8)).await?;
        reader.read_exact(&mut buf).await?;
        if !valid_signature_sansio(buf) {

        }
        todo!()
    }
}

fn valid_signature_sansio(buf: [u8; 8]) -> bool {
    buf == FILE_SIGNATURE
}
