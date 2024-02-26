use std::io;

use flatbuffers::InvalidFlatbuffer;

#[derive(Debug, thiserror::Error)]
pub enum Pod5Error {
    #[error("Failed to parse footer, {0}")]
    FooterParserFailure(#[from] InvalidFlatbuffer),

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
    RunInfoTable,

    #[error("{0}")]
    PolarsError(#[from] polars::prelude::PolarsError)
}
