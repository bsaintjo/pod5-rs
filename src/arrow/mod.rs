use std::{
    io::{Cursor, Read, Seek},
    marker::PhantomData,
};

use arrow::{
    array::{
        ArrayAccessor, ArrayIter, FixedSizeBinaryArray, Float32Array, LargeBinaryArray,
        RecordBatch, UInt16Array, UInt32Array,
    },
    error::ArrowError,
    ipc::reader::FileReader,
};

use crate::{footer::ParsedFooter, svb16::decode};

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

struct SignalData {
    raw_signal: Vec<i16>,
}

impl SignalData {
    fn from_raw_signal(raw_signal: &[i16]) -> Self {
        todo!()
    }
}

#[derive(Debug)]
struct ReadId {
    bytes: Vec<u8>,
}

impl ReadId {
    fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    fn from_slice(bytes: &[u8]) -> Self {
        Self::new(bytes.to_vec())
    }

    fn from_uuid(uuid: &str) -> Self {
        Self::from_slice(&uuid::Uuid::parse_str(uuid).unwrap().into_bytes())
    }

    fn raw(&self) -> &[u8] {
        &self.bytes
    }

    fn uuid(&self) -> String {
        uuid::Uuid::from_slice(&self.bytes).unwrap().to_string()
    }
}

#[derive(Debug)]
struct Record {
    read_id: ReadId,
    samples: u32,
    signal: Vec<i16>,
}
impl Record {
    fn new(read_id: ReadId, samples: u32, signal: Vec<i16>) -> Self {
        Self {
            read_id,
            samples,
            signal,
        }
    }
}

struct ReadBatchIterator<R> {
    reader: FileReader<R>,
    read_ids: Vec<ReadId>,
    signals: Vec<Vec<i16>>,
    samples: Vec<u32>,
}

impl<R> ReadBatchIterator<R>
where
    R: Read + Seek,
{
    fn load_next_batch(&mut self) -> Option<()> {
        let batch = self.reader.next()?.unwrap();
        let read_ids = ReadId::extract(&batch, "read_id");
        // let read_ids = batch
        //     .column_by_name("read_id")
        //     .unwrap()
        //     .as_any()
        //     .downcast_ref::<FixedSizeBinaryArray>()
        //     .unwrap()
        //     .iter()
        //     .map(|x| ReadId::new(x.unwrap().to_vec()))
        //     .collect::<Vec<_>>();
        let samples = u32::extract(&batch, "samples");
        // let samples = batch
        //     .column_by_name("samples")
        //     .unwrap()
        //     .as_any()
        //     .downcast_ref::<UInt32Array>()
        //     .unwrap()
        //     .iter()
        //     .map(|x| x.unwrap())
        //     .collect::<Vec<_>>();
        let signals = batch
            .column_by_name("signal")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .iter()
            .flatten()
            .zip(samples.iter())
            .map(|(compressed, &count)| decode(compressed, count as usize).unwrap())
            .collect::<Vec<_>>();

        self.read_ids = read_ids;
        self.signals = signals;
        self.samples = samples;
        Some(())
    }
}

impl<R> Iterator for ReadBatchIterator<R>
where
    R: Read + Seek,
{
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.read_ids.is_empty() && self.load_next_batch().is_none() {
            return None;
        }
        let read_id = self.read_ids.pop().unwrap();
        let signal = self.signals.pop().unwrap();
        let samples = self.samples.pop().unwrap();
        Some(Record::new(read_id, samples, signal))
    }
}

struct Reader {
    reader: FileReader<Cursor<Vec<u8>>>,
    footer: ParsedFooter,
}

struct ReaderMetadata {
    footer: ParsedFooter,
}

impl Reader {
    fn metadata(&self) -> ReaderMetadata {
        todo!()
    }

    fn from_reader<R>(mut reader: &mut R) -> Self
    where
        R: Read + Seek,
    {
        let parsed = ParsedFooter::read_footer(&mut *reader).unwrap();
        let st = parsed.signal_table().unwrap();
        let length = st.as_ref().length() as u64;

        let mut signal_buf = vec![0u8; length as usize];
        st.read_to_buf(&mut reader, &mut signal_buf).unwrap();
        let signal_buf = Cursor::new(signal_buf);

        let reader = Pod5ArrowReader::try_new(signal_buf).unwrap();
        let reader = reader.into_inner();
        Self {
            reader,
            footer: parsed,
        }
    }

    fn get_read(&self, read_id: &str) -> Record {
        todo!()
    }
    fn reads(self) -> ReadBatchIterator<Cursor<Vec<u8>>> {
        let mut rbi = ReadBatchIterator {
            reader: self.reader,
            read_ids: Vec::new(),
            signals: Vec::new(),
            samples: Vec::new(),
        };
        rbi.load_next_batch();
        rbi
    }
}

/// Generic trait for converting from Arrow arrays into corresponding vectors
trait ArrowExtract: private::Sealed {
    /// The specific Arrow Array type to convert into
    type ArrowArrayType;
    fn convert(int: Option<<&Self::ArrowArrayType as ArrayAccessor>::Item>) -> Self
    where
        for<'a> &'a Self::ArrowArrayType: ArrayAccessor;

    fn extract<'b>(batch: &'b RecordBatch, name: &str) -> Vec<Self>
    where
        Self: Sized,
        for<'a> &'a Self::ArrowArrayType: ArrayAccessor,
        for<'a> <Self as ArrowExtract>::ArrowArrayType: 'a,
    {
        let read_id = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Self::ArrowArrayType>()
            .unwrap();

        ArrayIter::new(read_id)
            .map(|x| Self::convert(x))
            .collect::<Vec<_>>()
    }
}

impl ArrowExtract for ReadId {
    type ArrowArrayType = FixedSizeBinaryArray;

    fn convert(int: std::option::Option<<&FixedSizeBinaryArray as ArrayAccessor>::Item>) -> Self {
        ReadId::new(int.unwrap().to_vec())
    }
}

macro_rules! extract_primitive {
    ($native_type:ty, $arr_type:ty) => {
        impl ArrowExtract for $native_type {
            type ArrowArrayType = $arr_type;

            fn convert(int: std::option::Option<<&$arr_type as ArrayAccessor>::Item>) -> Self {
                int.unwrap()
            }
        }
    };
}
extract_primitive!(u32, UInt32Array);
extract_primitive!(u16, UInt16Array);
extract_primitive!(f32, Float32Array);

struct Dict<T> {
    phantom: PhantomData<T>,
}

mod private {
    use super::ReadId;

    pub trait Sealed {}
    impl Sealed for ReadId {}
    impl Sealed for u32 {}
    impl Sealed for u16 {}
    impl Sealed for f32 {}
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, fs::File, io::Cursor};

    use arrow::array::{Int16DictionaryArray, LargeBinaryArray, StringArray, UInt32Array};
    use polars::prelude::ArrowDataType;
    use polars_arrow::{
        array::{growable::make_growable, Array, Utf8Array},
        compute::cast::utf8_to_utf8view,
    };

    use crate::{footer::ParsedFooter, svb16::decode};

    use super::*;

    #[test]
    #[ignore]
    fn test_read_reader() -> eyre::Result<()> {
        let path = "extra/multi_fast5_zip_v3.pod5";
        let mut file = File::open(path)?;
        let reader = Reader::from_reader(&mut file);
        for record in reader.reads() {
            println!(
                "read_id: {}, samples: {}",
                record.read_id.uuid(),
                record.samples
            );
        }

        Ok(())
        // todo!()
    }

    #[test]
    fn test_reader2() -> eyre::Result<()> {
        let path = "extra/multi_fast5_zip_v3.pod5";
        let mut file = File::open(path)?;
        let parsed = ParsedFooter::read_footer(&file)?;
        println!("footer: {:?}", parsed.footer());

        let rt = parsed.read_table()?;
        let length = rt.as_ref().length() as u64;

        let mut table_buf = vec![0u8; length as usize];
        rt.read_to_buf(&mut file, &mut table_buf)?;
        let signal_buf = Cursor::new(table_buf);
        let mut reader = FileReader::try_new(signal_buf, None)?;
        dbg!(reader.schema());
        let batch = reader.next().unwrap().unwrap();
        let pore_type = batch
            .column_by_name("pore_type")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16DictionaryArray>()
            .unwrap()
            .downcast_dict::<StringArray>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        Ok(())
    }

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
        let mut reader = reader.into_inner();
        println!("{}", reader.schema());
        let batch = reader.next().unwrap().unwrap();
        let read_id = batch
            .column_by_name("read_id")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .iter()
            .map(|x| uuid::Uuid::from_slice(x.unwrap()).unwrap().to_string())
            .collect::<Vec<_>>();
        let samples = batch
            .column_by_name("samples")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .iter()
            .map(|x| x.unwrap())
            .collect::<Vec<_>>();
        let signal = batch
            .column_by_name("signal")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .iter()
            .flatten()
            .zip(samples)
            .map(|(compressed, count)| decode(compressed, count as usize).unwrap())
            .collect::<Vec<_>>();

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
                let down = arr
                    .as_any()
                    .downcast_ref::<Utf8Array<i32>>()
                    .unwrap()
                    .clone();
                let arr = utf8_to_utf8view(&down);
                new_arrs.push(arr.to_boxed());
            }
            if f.dtype == ArrowDataType::LargeUtf8 {
                let down = arr
                    .as_any()
                    .downcast_ref::<Utf8Array<i64>>()
                    .unwrap()
                    .clone();
                let arr = utf8_to_utf8view(&down);
                new_arrs.push(arr.to_boxed());
            } else {
                new_arrs.push(arr);
            }
        }
    }
}
