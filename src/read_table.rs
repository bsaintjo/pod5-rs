use arrow2_convert::{ArrowDeserialize, ArrowField};

#[derive(ArrowField, ArrowDeserialize)]
pub(crate) struct SignalIdxs {
    idxs: Vec<Vec<u64>>,
}

impl SignalIdxs {
    fn new(idxs: Vec<Vec<u64>>) -> Self {
        Self { idxs }
    }

    fn from_arrow(options: Vec<Vec<Option<u64>>>) -> Self {
        SignalIdxs::new(
            options
                .into_iter()
                .map(|v| v.into_iter().flatten().collect())
                .collect(),
        )
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use arrow2_convert::deserialize::TryIntoCollection;

    use crate::reader::{read_embedded_arrow, Reader};

    // #[test]
    // fn test_table() -> eyre::Result<()> {
    //     let file = File::open("extra/multi_fast5_zip_v3.pod5")?;
    //     let mut reader = Reader::from_reader(file)?;
    //     let embedded_file = read_embedded_arrow(&mut reader.reader, reader.read_table)?;
    //     println!("{:?}", embedded_file.schema());
    //     let mut acc = Vec::new();
    //     for chunk in embedded_file {
    //         let chunk = chunk?;
    //         let arr = chunk.arrays();
    //         let dt = arr[1].as_ref().data_type();
    //         println!("{:?}", dt);
    //         let mut idxs: Vec<Vec<Option<u64>>> = arr[1].as_ref().try_into_collection()?;
    //         acc.append(&mut idxs);
    //     }
    //     println!("{:?}", acc);
    //     // for _read in reader.reads() {
    //     //     todo!()
    //     // }
    //     Ok(())
    // }
}
