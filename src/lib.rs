// #![feature(seek_stream_len)]
use std::{
    fs::File,
    io::{self, Cursor, Read, Seek, SeekFrom},
};

use error::Pod5Error;
use footer_generated::minknow::reads_format::EmbeddedFile;
pub use polars;
pub use polars_arrow;
use polars_arrow::io::ipc::read::{read_file_metadata, FileReader};

use crate::dataframe::compatibility::convert_array;

pub mod dataframe;
pub mod error;
pub mod footer;
pub mod footer_generated;
pub mod reader;
pub mod svb16;
mod arrow;

// #[derive(Debug, Clone, PartialEq, Eq)]
// struct SignalUuid(Vec<u8>);

// impl ArrowField for SignalUuid {
//     type Type = Self;

//     fn data_type() -> arrow2::datatypes::DataType {
//         arrow2::datatypes::DataType::Extension(
//             "minknow.uuid".to_string(),
//             Box::new(arrow2::datatypes::DataType::FixedSizeBinary(16)),
//             Some("".to_string()),
//         )
//     }
// }

// impl ArrowDeserialize for SignalUuid {
//     type ArrayType = arrow2::array::FixedSizeBinaryArray;

//     fn arrow_deserialize(
//         v: <&Self::ArrayType as IntoIterator>::Item,
//     ) -> Option<<Self as ArrowField>::Type> {
//         v.map(|t| SignalUuid(t.to_vec()))
//     }
// }

// #[derive(Debug, Clone, PartialEq, Eq)]
// struct SignalVbz(Vec<u8>);

// impl ArrowField for SignalVbz {
//     type Type = Self;

//     fn data_type() -> arrow2::datatypes::DataType {
//         arrow2::datatypes::DataType::Extension(
//             "minknow.vbz".to_string(),
//             Box::new(arrow2::datatypes::DataType::LargeBinary),
//             Some("".to_string()),
//         )
//     }
// }

// impl ArrowDeserialize for SignalVbz {
//     type ArrayType = arrow2::array::BinaryArray<i64>;

//     fn arrow_deserialize(
//         v: <&Self::ArrayType as IntoIterator>::Item,
//     ) -> Option<<Self as ArrowField>::Type> {
//         v.map(|t| SignalVbz(t.to_vec()))
//     }
// }

// #[derive(Debug, Clone, PartialEq, Eq, ArrowDeserialize)]
// struct SignalUncompressed(Vec<i16>);

// impl ArrowField for SignalUncompressed {
//     type Type = Self;

//     fn data_type() -> arrow2::datatypes::DataType {
//         arrow2::datatypes::DataType::LargeList(Box::new(arrow2::datatypes::Field::new(
//             "signal",
//             arrow2::datatypes::DataType::Int16,
//             true,
//         )))
//     }
// }

// #[derive(Debug, Clone, PartialEq, ArrowField, ArrowDeserialize)]
// struct SignalRow {
//     read_id: Option<SignalUuid>,
//     signal: Option<SignalVbz>,
//     samples: Option<u32>,
// }

// struct SignalRowRef<'a> {
//     read_id: &'a [Option<SignalUuid>],
//     signal: &'a [Option<SignalVbz>],
//     samples: &'a [Option<u32>],
// }

// impl SignalRow {
//     fn schema() -> Schema {
//         let dt = Self::data_type();
//         Schema::from(vec![arrow2::datatypes::Field::new("test", dt, false)])
//     }
// }

const FILE_SIGNATURE: [u8; 8] = [0x8b, b'P', b'O', b'D', b'\r', b'\n', 0x1a, b'\n'];

fn check_signature<R>(mut reader: R) -> Result<bool, io::Error>
where
    R: Read + Seek,
{
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(buf == FILE_SIGNATURE)
}

fn read_footer(mut file: &File) -> Result<Vec<u8>, io::Error> {
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

fn to_dataframe<R: Read + Seek>(efile: &EmbeddedFile, mut file: R) -> Result<(), Pod5Error> {
    let offset = efile.offset() as u64;
    let length = efile.length() as u64;
    let mut run_info_buf = vec![0u8; length as usize];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut run_info_buf)?;
    let mut run_info_buf = Cursor::new(run_info_buf);
    let metadata =
        read_file_metadata(&mut run_info_buf).map_err(|_| Pod5Error::SignalTableMissing)?;
    let fields = metadata.schema.clone();

    let signal_table = FileReader::new(run_info_buf, metadata.clone(), None, None);
    for table in signal_table {
        if let Ok(chunk) = table {
            let mut acc = Vec::new();
            for (arr, f) in chunk.arrays().iter().zip(fields.iter()) {
                let arr = convert_array(arr.as_ref());
                let s = polars::prelude::Series::try_from((f.1, arr));
                acc.push(s.unwrap());
            }

            let df = polars::prelude::DataFrame::from_iter(acc.into_iter());
            println!("runinfo {df}");
        } else {
            println!("Error read table!")
        }
    }
    Ok(())
}

#[cfg(doctest)]
doc_comment::doctest!("../README.md", readme);

#[cfg(test)]
mod tests {

    use std::{fs::File, io::Cursor};

    // use arrow2::io::ipc::read::read_file_metadata;
    use flatbuffers::root;
    use memmap2::MmapOptions;
    use polars_arrow::io::ipc::read::read_file_metadata;

    use super::*;
    use crate::{
        dataframe::compatibility::convert_array2, footer_generated::minknow::reads_format::Footer,
    };

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
    fn test_read_footer2() -> eyre::Result<()> {
        let path = "extra/multi_fast5_zip_v3.pod5";
        let file = File::open(path)?;
        let data = read_footer(&file)?;
        let footer = root::<Footer>(&data)?;
        println!("{footer:?}");
        Ok(())
    }

    #[test]
    fn test_read_footer_polars() -> eyre::Result<()> {
        let path = "extra/multi_fast5_zip_v3.pod5";
        let mut file = File::open(path)?;
        let data = read_footer(&file)?;
        let footer = root::<Footer>(&data)?;
        println!("footer: {footer:?}");
        let embedded = footer.contents().unwrap();
        let efile = embedded.get(0);
        let offset = efile.offset() as u64;
        let length = efile.length() as u64;
        let mut signal_buf = vec![0u8; length as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut signal_buf)?;

        let mut signal_buf = Cursor::new(signal_buf);
        let metadata = polars_arrow::io::ipc::read::read_file_metadata(&mut signal_buf)?;
        let fields = metadata.schema.clone();
        println!("metadata schema: {:?}", &metadata.schema);
        println!("metadata ipc schema: {:?}", &metadata.ipc_schema);

        let signal_table =
            polars_arrow::io::ipc::read::FileReader::new(signal_buf, metadata.clone(), None, None);
        for table in signal_table {
            if let Ok(chunk) = table {
                let mut acc = Vec::new();
                for (arr, f) in chunk.into_arrays().into_iter().zip(fields.iter()) {
                    let arr = convert_array2(arr);
                    let s = polars::prelude::Series::try_from((f.1, arr));
                    acc.push(s.unwrap());
                }

                let df = polars::prelude::DataFrame::from_iter(acc.into_iter());
                println!("signal df {df}");
            } else {
                println!("Error!")
            }
        }

        let read_efile = embedded.get(2);
        to_dataframe(&read_efile, &file).unwrap();

        let run_info_efile = embedded.get(1);
        to_dataframe(&run_info_efile, &file).unwrap();

        Ok(())
    }

    // #[test]
    // fn test_read_footer() -> eyre::Result<()> {
    //     let path = "extra/multi_fast5_zip_v3.pod5";
    //     let mut file = File::open(path)?;
    //     let data = read_footer(&file)?;
    //     let footer = root::<Footer>(&data)?;
    //     println!("footer: {footer:?}");
    //     let embedded = footer.contents().unwrap();
    //     let efile = embedded.get(0);
    //     let offset = efile.offset() as u64;
    //     let length = efile.length() as u64;
    //     let mut signal_buf = Vec::new();
    //     signal_buf.resize(length as usize, 0x0);
    //     file.seek(SeekFrom::Start(offset))?;
    //     file.read_exact(&mut signal_buf)?;

    //     let mut signal_buf = Cursor::new(signal_buf);
    //     let metadata = read_file_metadata(&mut signal_buf)?;
    //     // println!("from metadata: {:?}\n", metadata.schema);
    //     // println!("from SignalRow: {:?}\n", SignalRow::schema());
    //     let signal_table = FileReader::new(signal_buf, metadata, None, None);
    //     println!("from signal table: {:?}\n", signal_table.schema());

    //     let read_efile = embedded.get(2);
    //     let offset = read_efile.offset() as u64;
    //     let length = read_efile.length() as u64;
    //     let mut read_buf = Vec::new();
    //     read_buf.resize(length as usize, 0x0);
    //     file.seek(SeekFrom::Start(offset))?;
    //     file.read_exact(&mut read_buf)?;
    //     // let mut read_buf = Cursor::new(read_buf);

    //     // let metadata = read_file_metadata(&mut read_buf)?;
    //     // let mut read_table = FileReader::new(read_buf, metadata, None, None);
    //     // println!("from read table: {:?}\n", read_table.schema());
    //     // let chunk = read_table.next().unwrap().unwrap();
    //     // let arrs = chunk.arrays();
    //     // let num_samples_arr = arrs[12].as_ref();
    //     // println!("num samples {:?}", num_samples_arr.data_type());
    //     // let num_samples: Vec<u64> = num_samples_arr.try_into_collection()?;
    //     // println!("num samples {:?}", num_samples);

    //     for table in signal_table {
    //         if let Ok(chunk) = table {
    //             let arr_iter = chunk.arrays();
    //             // let uuid: Vec<Option<SignalUuid>> =
    // arr_iter[0].as_ref().try_into_collection()?;             let vbz:
    // Vec<Option<SignalVbz>> = arr_iter[1].as_ref().try_into_collection()?;
    //             let samples: Vec<Option<u32>> =
    // arr_iter[2].as_ref().try_into_collection()?;
    // println!("samples {samples:?}");             let data: &[u8] =
    // vbz[0].as_ref().unwrap().0.as_ref();             // let rest: &[u8] =
    // vbz[1].as_ref().unwrap().0.as_ref();             // let mut total_data =
    // data.to_vec();             // total_data.extend_from_slice(rest);
    //             // let total_data = Cursor::new(total_data);
    //             // let res = zstd::decode_all(total_data).unwrap();
    //             // println!("combined zstd decoded len {}", res.len());
    //             let count = samples[0].unwrap() as usize;
    //             println!("count: {count}");
    //             // let _decoded = svb::decode(data, count)?;
    //             let decoded = svb16::decode(data, count).unwrap();
    //             println!("decoded len: {}", decoded.len());
    //             // println!("decoded: {decoded:?}");
    //             // let ref_rows = SignalRowRef { read_id: &uuid, signal: &vbz,
    // samples: &samples };             // for arr in
    // chunk.into_arrays().into_iter() {             //     let row:
    // Vec<SignalRow> = arr.try_into_collection()?;             // }
    //         } else {
    //             println!("Failed");
    //         }
    //     }
    //     // let row = signal_table.next().unwrap().unwrap();
    //     // let arr = row.into_arrays().into_iter().next().unwrap();
    //     // let datatype = arr.data_type();
    //     // let ptype = datatype.to_physical_type();
    //     // println!("{arr:?}");
    //     // println!("Datatype:\t{datatype:?}");
    //     // println!("Physical Type:\t{ptype:?}");
    //     // let arr: Result<Vec<SignalRow>, _> = arr.try_into_collection();
    //     // println!("{:?}", SignalRow::data_type());
    //     // println!("{arr:?}");

    //     Ok(())
    // }

    #[test]
    fn test_check_signature() -> eyre::Result<()> {
        let path = "extra/multi_fast5_zip_v0.pod5";
        let mut file = File::open(path)?;
        assert!(check_signature(&file)?);
        file.seek(SeekFrom::End(-8))?;
        assert!(check_signature(&file)?);
        Ok(())
    }
}
