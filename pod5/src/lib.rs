// #![feature(seek_stream_len)]
pub use polars;
pub use polars_arrow;

pub mod dataframe;
pub mod error;
pub mod reader;
pub mod writer;

const FILE_SIGNATURE: [u8; 8] = [0x8b, b'P', b'O', b'D', b'\r', b'\n', 0x1a, b'\n'];

#[cfg(doctest)]
doc_comment::doctest!("../../README.md", readme);

#[cfg(test)]
mod tests {

    use std::{
        fs::File,
        io::{self, Cursor, Read, Seek, SeekFrom},
    };

    use memmap2::MmapOptions;
    use polars_arrow::io::ipc::read::read_file_metadata;

    use super::*;

    #[test]
    fn test_pod5() -> eyre::Result<()> {
        let path = "../extra/multi_fast5_zip_v0.pod5";
        let reader = File::open(path)?;

        let mmap = unsafe { MmapOptions::new().map(&reader)? };
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

    // #[test]
    // fn test_read_footer2() -> eyre::Result<()> {
    //     let path = "extra/multi_fast5_zip_v3.pod5";
    //     let file = File::open(path)?;
    //     let data = read_footer(&file)?;
    //     let footer = root::<Footer>(&data)?;
    //     println!("{footer:?}");
    //     Ok(())
    // }

    // #[test]
    // fn test_read_footer_polars() -> eyre::Result<()> {
    //     let path = "extra/multi_fast5_zip_v3.pod5";
    //     let mut file = File::open(path)?;
    //     let data = read_footer(&file)?;
    //     let footer = root::<Footer>(&data)?;
    //     println!("footer: {footer:?}");
    //     let embedded = footer.contents().unwrap();
    //     let efile = embedded.get(0);
    //     let offset = efile.offset() as u64;
    //     let length = efile.length() as u64;
    //     let mut signal_buf = vec![0u8; length as usize];
    //     file.seek(SeekFrom::Start(offset))?;
    //     file.read_exact(&mut signal_buf)?;

    //     let mut signal_buf = Cursor::new(signal_buf);
    //     let metadata = polars_arrow::io::ipc::read::read_file_metadata(&mut
    // signal_buf)?;     let fields = metadata.schema.clone();
    //     println!("metadata schema: {:?}", &metadata.schema);
    //     println!("metadata ipc schema: {:?}", &metadata.ipc_schema);

    //     let signal_table =
    //         polars_arrow::io::ipc::read::FileReader::new(signal_buf,
    // metadata.clone(), None, None);     for table in signal_table {
    //         if let Ok(chunk) = table {
    //             let mut acc = Vec::new();
    //             for (arr, f) in
    // chunk.into_arrays().into_iter().zip(fields.iter()) {                 let
    // s = array_to_series(f.1, arr);                 acc.push(s);
    //             }

    //             let df = polars::prelude::DataFrame::from_iter(acc.into_iter());
    //             println!("signal df {df}");
    //         } else {
    //             println!("Error!")
    //         }
    //     }
    //     let read_efile = embedded.get(0);
    //     to_dataframe(&read_efile, &file).unwrap();

    //     let read_efile = embedded.get(2);
    //     to_dataframe(&read_efile, &file).unwrap();

    //     let run_info_efile = embedded.get(1);
    //     to_dataframe(&run_info_efile, &file).unwrap();

    //     Ok(())
    // }
    fn check_signature<R>(mut reader: R) -> Result<bool, io::Error>
    where
        R: Read + Seek,
    {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(buf == FILE_SIGNATURE)
    }

    #[test]
    fn test_check_signature() -> eyre::Result<()> {
        let path = "../extra/multi_fast5_zip_v0.pod5";
        let mut file = File::open(path)?;
        assert!(check_signature(&file)?);
        file.seek(SeekFrom::End(-8))?;
        assert!(check_signature(&file)?);
        Ok(())
    }
}
