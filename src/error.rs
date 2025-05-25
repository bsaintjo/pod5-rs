//! Error types.
use std::io;

use flatbuffers::InvalidFlatbuffer;
use polars::error::PolarsError;

#[derive(Debug, thiserror::Error)]
pub enum Pod5Error {
    /// Reason why flatbuffers failed to parse the footer
    #[error("Failed to parse footer, {0}")]
    FooterParserFailure(#[from] InvalidFlatbuffer),

    /// The signature at the beginning or ending of the file wasn't able to be
    /// verified. This may mean that the file was corrupted or incorrectly
    /// written.
    #[error("Failed to verify signature: {0}")]
    SignatureFailure(&'static str),

    #[error("{0}")]
    IOError(#[from] io::Error),

    #[error("Missing list of embedded files from footer, footer is likely improperly constructed or pod5 is empty")]
    ContentsMissing,

    #[error("Missing Signal table from POD5")]
    SignalTableMissing,

    #[error("Missing Read table from POD5")]
    ReadTableMissing,

    #[error("Missing Run Info table from POD5")]
    RunInfoTableMissing,

    #[error("Problem with reading metadata: {0}")]
    ReadMetadataError(PolarsError),

    /// Error occured in the DataFrame API from polars
    #[error("{0}")]
    PolarsError(#[from] polars::prelude::PolarsError),
}
