use std::{
    collections::HashMap,
    io::{Read, Seek},
};

use lru::LruCache;
use polars::prelude::ArrowField;
use polars_arrow::{
    array::{Array, FixedSizeBinaryArray},
    io::ipc::read::FileReader,
    record_batch::RecordBatchT,
};
use uuid::Uuid;

use super::SignalDataFrame;

struct SignalReadIndexer<R: Read + Seek> {
    read_index: HashMap<String, Vec<u64>>,
    batches: LruCache<u64, RecordBatchT<Box<dyn Array>>>,
    reader: FileReader<R>,
}

#[derive(Debug, thiserror::Error)]
enum IndexerError {
    #[error("No read_id column was found.")]
    NoMinknowUuid,
}

impl<R> SignalReadIndexer<R>
where
    R: Read + Seek,
{
    fn from_reader(reader: FileReader<R>) -> Result<Self, IndexerError> {
        let mut read_index = HashMap::new();
        let fields: Vec<(usize, &ArrowField)> = reader
            .metadata()
            .schema
            .iter()
            .map(|f| f.1)
            .enumerate()
            .filter(|f| f.1.name == "read_id")
            .collect();

        if fields.len() != 1 {
            return Err(IndexerError::NoMinknowUuid);
        }

        let read_id_col_index = fields[0].0;

        for (batch_index, record_batch) in reader.enumerate() {
            record_batch
                .unwrap()
                .get(read_id_col_index)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .values_iter()
                .map(|x| Uuid::from_slice(x).unwrap().to_string())
                .for_each(|rid| {
                    read_index.insert(rid, batch_index);
                });
        }
        todo!()
    }

    fn get_read(read_id: &str) -> SignalDataFrame {
        todo!()
    }
}
