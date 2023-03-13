use arrow2::datatypes::{Field, Schema};
use arrow2_convert::{
    arrow_enable_vec_for_type, deserialize::ArrowDeserialize, field::ArrowField, ArrowDeserialize,
    ArrowField,
};

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

mod footer_generated;
mod svb;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SignalUuid(Vec<u8>);

impl ArrowField for SignalUuid {
    type Type = Self;

    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Extension(
            "minknow.uuid".to_string(),
            Box::new(arrow2::datatypes::DataType::FixedSizeBinary(16)),
            Some("".to_string()),
        )
    }
}

impl ArrowDeserialize for SignalUuid {
    type ArrayType = arrow2::array::FixedSizeBinaryArray;

    fn arrow_deserialize(
        v: <&Self::ArrayType as IntoIterator>::Item,
    ) -> Option<<Self as ArrowField>::Type> {
        v.map(|t| SignalUuid(t.to_vec()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SignalVbz(Vec<i16>);

impl ArrowField for SignalVbz {
    type Type = Self;

    fn data_type() -> arrow2::datatypes::DataType {
        arrow2::datatypes::DataType::Extension(
            "minknow.vbz".to_string(),
            Box::new(arrow2::datatypes::DataType::LargeBinary),
            Some("".to_string()),
        )
    }
}

impl ArrowDeserialize for SignalVbz {
    type ArrayType = arrow2::array::BinaryArray<i64>;

    fn arrow_deserialize(
        v: <&Self::ArrayType as IntoIterator>::Item,
    ) -> Option<<Self as ArrowField>::Type> {
        v.map(|t| SignalVbz(t.iter().map(|&x| x as i16).collect()))
    }
}

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowDeserialize)]
struct SignalRow {
    read_id: Option<SignalUuid>,
    signal: Option<SignalVbz>,
    samples: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, ArrowField, ArrowDeserialize)]
struct SignalRow2(Option<SignalUuid>, Option<SignalVbz>, Option<u32>);

impl SignalRow {
    fn schema() -> Schema {
        let dt = Self::data_type();
        Schema::from(vec![Field::new("test", dt, false)])
    }
}

arrow_enable_vec_for_type!(SignalUuid);
arrow_enable_vec_for_type!(SignalVbz);
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

fn check_signature<R>(mut reader: R) -> eyre::Result<bool>
where
    R: Read + Seek,
{
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
    use std::{borrow::Cow, fs::File, io::Cursor};

    use arrow2::io::ipc::read::{read_file_metadata, FileReader};
    use arrow2_convert::deserialize::TryIntoCollection;
    use flatbuffers::root;
    use memmap2::MmapOptions;

    use crate::footer_generated::minknow::reads_format::Footer;

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
        let mut file = File::open(path)?;
        let data = read_footer(&file)?;
        let footer = root::<Footer>(&data)?;
        let embedded = footer.contents().unwrap();
        let efile = embedded.get(0);
        let offset = efile.offset() as u64;
        let length = efile.length() as u64;
        let mut signal_buf = vec![0u8; length as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut signal_buf)?;

        let mut signal_buf = Cursor::new(signal_buf);
        let metadata = read_file_metadata(&mut signal_buf)?;
        // println!("from metadata: {:?}\n", metadata.schema);
        println!("from SignalRow: {:?}\n", SignalRow::schema());
        let signal_table = FileReader::new(signal_buf, metadata, None, None);

        println!("from filereader: {:?}\n", signal_table.schema());
        for table in signal_table {
            if let Ok(chunk) = table {
                let arr_iter = chunk.arrays();
                let _uuid: Cow<[SignalUuid]> = arr_iter[0].as_ref().try_into_collection()?;
                // let vbz: Vec<SignalVbz> = arr_iter.next().unwrap().try_into_collection()?;
                // let samples: Vec<u32> = arr_iter.next().unwrap().try_into_collection()?;
                // for arr in chunk.into_arrays().into_iter() {
                //     let row: Vec<SignalRow> = arr.try_into_collection()?;
                // }
            } else {
                println!("Failed");
            }
        }
        // let row = signal_table.next().unwrap().unwrap();
        // let arr = row.into_arrays().into_iter().next().unwrap();
        // let datatype = arr.data_type();
        // let ptype = datatype.to_physical_type();
        // println!("{arr:?}");
        // println!("Datatype:\t{datatype:?}");
        // println!("Physical Type:\t{ptype:?}");
        // let arr: Result<Vec<SignalRow>, _> = arr.try_into_collection();
        // println!("{:?}", SignalRow::data_type());
        // println!("{arr:?}");

        Ok(())
    }

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
