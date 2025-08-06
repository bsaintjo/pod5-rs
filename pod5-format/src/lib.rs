mod error;
pub mod footer;

use std::io::{Read, Seek};

pub use error::FormatError;
pub use footer::ParsedFooter;
pub use footer::FooterBuilder;
pub use footer::FOOTER_MAGIC;
pub use footer::TableInfo;

pub use footer::footer_generated;

pub const FILE_SIGNATURE: [u8; 8] = [0x8b, b'P', b'O', b'D', b'\r', b'\n', 0x1a, b'\n'];

pub fn valid_signature(buf: &[u8]) -> bool {
    buf == FILE_SIGNATURE
}
