use std::io::{Read, Seek};

use arrow::{
    array::{ArrayIter, AsArray, FixedSizeBinaryArray, RecordBatch},
    error::ArrowError,
    ipc::reader::FileReader,
};

#[derive(thiserror::Error, Debug)]
pub enum Pod5ArrowError {
    #[error("Arrow error: {0}")]
    ArrowError(#[from] ArrowError),
}

#[derive(Debug)]
pub struct Pod5ArrowReader<R> {
    reader: FileReader<R>,
}

impl<R> Pod5ArrowReader<R>
where
    R: Read + Seek,
{
    pub fn try_new(reader: R) -> Result<Self, Pod5ArrowError> {
        let reader = FileReader::try_new(reader, None)?;
        Ok(Self { reader })
    }

    pub fn into_inner(self) -> FileReader<R> {
        self.reader
    }
}

impl<R> Iterator for Pod5ArrowReader<R> {
    type Item = ArrowBatch;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct ArrowBatch {
    batch: RecordBatch,
}

impl ArrowBatch {
    fn signal_data(&self) -> SignalData {
        todo!()
    }

    fn samples_data(&self) -> SamplesData {
        todo!()
    }

    fn read_id_data(&self) -> ReadIdData {
        let read_id_col = self
            .batch
            .column_by_name("read_id")
            .unwrap()
            .as_fixed_size_binary();
        todo!()
    }

    // fn into_inner(self) -> RecordBatch {
    //     todo!()
    // }
}

struct SignalData;

impl SignalData {
    fn decompressed_signal_iter(&self) -> DecompressedSignalIterator {
        todo!()
    }
}

struct DecompressedSignalIterator {}

impl Iterator for DecompressedSignalIterator {
    type Item = Option<Vec<i16>>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

struct SamplesData;

struct ReadIdData;

struct ReadIdIterator {}

impl Iterator for ReadIdIterator {
    type Item = Option<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl IntoIterator for ReadIdData {
    type Item = Option<Vec<u8>>;

    type IntoIter = ReadIdIterator;

    fn into_iter(self) -> Self::IntoIter {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        fs::File,
        io::{Cursor, SeekFrom},
    };

    use polars::prelude::ArrowDataType;
    use polars_arrow::{
        array::{growable::make_growable, Array, BooleanArray, Int32Array, Utf8Array}, compute::cast::utf8_to_utf8view, datatypes::PhysicalType, offset::OffsetsBuffer
    };

    use crate::footer::ParsedFooter;

    use super::*;

    #[test]
    fn test_reader() -> eyre::Result<()> {
        let path = "extra/multi_fast5_zip_v3.pod5";
        let mut file = File::open(path)?;
        let parsed = ParsedFooter::read_footer(&file)?;
        println!("footer: {:?}", parsed.footer());

        let st = parsed.signal_table()?;
        let length = st.as_ref().length() as u64;

        let mut signal_buf = vec![0u8; length as usize];
        st.read_to_buf(&mut file, &mut signal_buf)?;
        let signal_buf = Cursor::new(signal_buf);

        let reader = Pod5ArrowReader::try_new(signal_buf)?;

        // for batch in reader {
        //     for (uuid, signal) in batch.read_id_data().into_iter().zip(batch.signal_data().decompressed_signal_iter()) {
        //         todo!()
        //     }
        // }

        Ok(())
    }

    fn dt() -> ArrowDataType {
        ArrowDataType::Struct(vec![
            polars_arrow::datatypes::Field::new("a".into(), ArrowDataType::Utf8, true),
            polars_arrow::datatypes::Field::new("b".into(), ArrowDataType::Utf8, true),
        ])
    }

    fn array() -> polars_arrow::array::MapArray {
        let dtype = ArrowDataType::Map(
            Box::new(polars_arrow::datatypes::Field::new("a".into(), dt(), true)),
            false,
        );

        let field = polars_arrow::array::StructArray::new(
            dt(),
            3,
            vec![
                Box::new(polars_arrow::array::Utf8Array::<i32>::from_slice([
                    "a", "aa", "aaa",
                ])) as _,
                Box::new(polars_arrow::array::Utf8Array::<i32>::from_slice([
                    "b", "bb", "bbb",
                ])),
            ],
            None,
        );

        polars_arrow::array::MapArray::new(
            dtype,
            vec![0, 1, 2, 3].try_into().unwrap(),
            Box::new(field),
            None,
        )
    }

    #[test]
    #[ignore = "exploring API, will tend panic"]
    fn test_map_conversion() {
        let map_array = array();
        let x = map_array.iter().next().unwrap().unwrap();
        println!("{x:?}");
        let sa: &polars_arrow::array::StructArray = x.as_any().downcast_ref().unwrap();
        let mut dict = HashMap::new();
        let (fields, capacity, data, validity) = sa.clone().into_data();
        for (field, datum) in fields.into_iter().zip(data.into_iter()) {
            dict.entry(field).or_insert(Vec::new()).push(datum);
            // make_growable(arrays, use_validity, capacity)
        }
        let mut new_arrs = Vec::new();
        for (f, arrs) in dict.into_iter() {
            let acc = arrs.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
            let arr = make_growable(&acc, false, capacity).as_box();

            if f.dtype == ArrowDataType::Utf8 {
                let down = arr.as_any().downcast_ref::<Utf8Array<i32>>().unwrap().clone();
                let arr = utf8_to_utf8view(&down);
                new_arrs.push(arr.to_boxed());
            }
            if f.dtype == ArrowDataType::LargeUtf8 {
                let down = arr.as_any().downcast_ref::<Utf8Array<i64>>().unwrap().clone();
                let arr = utf8_to_utf8view(&down);
                new_arrs.push(arr.to_boxed());
            } else {
                new_arrs.push(arr);
            }
        }
    }
}
