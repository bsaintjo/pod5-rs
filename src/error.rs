use std::io;

use flatbuffers::InvalidFlatbuffer;

#[derive(Debug, thiserror::Error)]
pub enum Pod5Error {
    #[error("Failed to parse footer, {0}")]
    FooterParserFailure(#[from] InvalidFlatbuffer),

    #[error("Failed to verify signature")]
    SignatureFailure,

    #[error("{0}")]
    IOError(#[from] io::Error),

    #[error("Missing list of embedded files from footer, footer is likely improperly constructed or pod5 is empty")]
    ContentsMissing,
}
