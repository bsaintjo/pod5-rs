use std::io::{Cursor, Read, Seek};

use arrow::{array::LargeBinaryArray, error::ArrowError, ipc::reader::FileReader};
use extract::ArrowExtract;

use crate::{footer::ParsedFooter, svb16::decode};

mod extract;
mod record;

#[derive(thiserror::Error, Debug)]
pub enum Pod5ArrowError {
    #[error("Arrow error: {0}")]
    ArrowError(#[from] ArrowError),
}

struct ReadBatchIterator<R> {
    reader: FileReader<R>,
    read_ids: Vec<record::ReadId>,
    signals: Vec<Vec<i16>>,
    samples: Vec<u32>,
}

impl<R> ReadBatchIterator<R>
where
    R: Read + Seek,
{
    fn load_next_batch(&mut self) -> Option<()> {
        let batch = self.reader.next()?.unwrap();
        let read_ids = record::ReadId::extract(&batch, "read_id");
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
    type Item = record::Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.read_ids.is_empty() && self.load_next_batch().is_none() {
            return None;
        }
        let read_id = self.read_ids.pop().unwrap();
        let signal = record::SignalData::from_raw_signal(self.signals.pop().unwrap());
        let samples = self.samples.pop().unwrap();
        Some(record::Record::new(read_id, samples, signal))
    }
}

struct Reader {
    reader: FileReader<Cursor<Vec<u8>>>,
    footer: ParsedFooter,
}

impl Reader {
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

        let reader = FileReader::try_new(signal_buf, None).unwrap();
        Self {
            reader,
            footer: parsed,
        }
    }

    fn get_read(&self, read_id: &str) -> record::Record {
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

#[cfg(test)]
mod test {
    use std::{collections::HashMap, fs::File, io::Cursor};

    use arrow::array::{
        FixedSizeBinaryArray, Int16DictionaryArray, LargeBinaryArray, ListArray, StringArray,
        UInt32Array, UInt64Array,
    };
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
        dbg!(&batch);
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

        let signal_idxs = batch
            .column_by_name("signal")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .iter()
            .map(|x| {
                x.unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        // .downcast_dict::<StringArray>()
        // .unwrap()
        // .into_iter()
        // .flatten()
        // .collect::<Vec<_>>();

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

        let mut reader = FileReader::try_new(signal_buf, None)?;
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
