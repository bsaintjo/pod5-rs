mod footer_generated;
mod reader;
mod error;

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

struct ParsedFooter {
    data: Vec<u8>,
}

impl ParsedFooter {
    fn footer(&self) -> eyre::Result<Footer<'_>> {
        Ok(root::<Footer>(&self.data)?)
    }
}

use flatbuffers::root;
use footer_generated::minknow::reads_format::Footer;

const FILE_SIGNATURE: [u8; 8] = [0x8b, b'P', b'O', b'D', b'\r', b'\n', 0x1a, b'\n'];

fn check_signature<R>(mut reader: R) -> eyre::Result<bool> where R: Read + Seek {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(buf == FILE_SIGNATURE)
}

fn read_footer(mut file: &File) -> eyre::Result<Vec<u8>> {
    let file_size = file.metadata()?.len();
    let footer_length_end: u64 = (file_size - FILE_SIGNATURE.len() as u64) - 16;
    let footer_length = footer_length_end - 8;
    file.seek(SeekFrom::Start(footer_length))?;
    let mut buf = [0; 8];
    file.read_exact(&mut buf)?;
    let flen = i64::from_le_bytes(buf);
    file.seek(SeekFrom::Start(footer_length - (flen as u64)))?;
    let mut buf = vec![0u8; flen as usize];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Cursor};

    use arrow2::io::ipc::read::read_file_metadata;
    use flatbuffers::{root, FlatBufferBuilder};
    use memmap2::MmapOptions;

    use super::*;

    #[test]
    fn test_pod5() -> eyre::Result<()> {
        let path = "extra/multi_fast5_zip_v0.pod5";
        let reader = File::open(path)?;
        let mut mmap = unsafe { MmapOptions::new().map(&reader)? };
        #[cfg(target_family = "unix")]
        mmap.lock()?;
        let mut res = Err(());
        for i in 25..mmap.len() {
            let mut section = Cursor::new(&mmap[24..i]);
            if let Ok(m) = read_file_metadata(&mut section) {
                res = Ok((i, m));
                break;
            }
        }
        assert!(res.is_ok());
        let (i, m) = res.unwrap();
        println!("file length\t\t\t{}", mmap.len());
        println!("end position of valid arrow\t{i:?}");
        println!("{m:#?}");
        Ok(())
    }

    #[test]
    fn test_read_footer() -> eyre::Result<()> {
        let path = "extra/multi_fast5_zip_v0.pod5";
        let file = File::open(path)?;
        let data = read_footer(&file)?;
        let footer = root::<Footer>(&data)?;
        println!("{footer:?}");
        Ok(())
    }

    #[test]
    fn test_check_signature() -> eyre::Result<()>{
        let path = "extra/multi_fast5_zip_v0.pod5";
        let mut file = File::open(path)?;
        assert!(check_signature(&file)?);
        file.seek(SeekFrom::End(-8))?;
        assert!(check_signature(&file)?);
        Ok(())
    }
}
