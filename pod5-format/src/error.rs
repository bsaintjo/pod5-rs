use flatbuffers::InvalidFlatbuffer;

#[derive(thiserror::Error, Debug)]
pub enum FormatError {
    #[error("Error encountered in Footer operation: {0}")]
    FooterError(#[from] FooterError)
}

#[derive(thiserror::Error, Debug)]
pub enum FooterError {
    /// Error from the FlatBuffer parser
    #[error("FlatBuffers error: {0}")]
    FlatBuffersError(#[from] InvalidFlatbuffer),

    /// An IO error occurred during parsing of the POD5 file
    #[error("Footer IO Error: {0}")]
    FooterIOError(#[from] std::io::Error),

    #[error(
        "Missing list of embedded files from footer, footer is likely improperly constructed or POD5 is empty"
    )]
    ContentsMissing,

    /// Failed to find the Signal Table
    #[error("Missing Signal table from POD5")]
    SignalTableMissing,

    /// Failed to find the Read Table
    #[error("Missing Read table from POD5")]
    ReadTableMissing,

    /// Failed to find the Run Info Table
    #[error("Missing Run Info table from POD5")]
    RunInfoTableMissing,
}
