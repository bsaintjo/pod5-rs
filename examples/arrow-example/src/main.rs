use std::{
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
    sync::Arc,
};

use arrow::{
    array::{Array, AsArray, GenericByteArray, GenericListArray, PrimitiveArray},
    datatypes::{GenericBinaryType, UInt32Type, UInt64Type},
    ipc::reader::FileReader,
};
use pod5::svb16;
use pod5_footer::ParsedFooter;

fn get_reads_info(buf: &[u8]) -> eyre::Result<Vec<Arc<dyn Array>>> {
    let cursor = Cursor::new(buf);
    let reader = FileReader::try_new(cursor, None)?;
    println!("schema: {}", reader.schema());
    let mut acc = Vec::new();
    for batch in reader {
        let batch = batch?;
        let signal_ids: &GenericListArray<i32> = batch.column_by_name("signal").unwrap().as_list();
        for read in signal_ids.iter() {
            let read = read.unwrap();
            acc.push(read.clone());
            let read: &PrimitiveArray<UInt64Type> = read.as_primitive();
            println!("read: {read:?}");
        }
    }
    Ok(acc)
}

fn main() -> eyre::Result<()> {
    let path = "extra/multi_fast5_zip_v3.pod5";
    let mut file = File::open(path)?;
    let parsed = ParsedFooter::read_footer(&file)?;

    println!("footer: {:?}", parsed.footer());

    let rt = parsed.read_table()?;
    let mut read_buf = vec![0u8; rt.as_ref().length().try_into().unwrap()];
    file.seek(SeekFrom::Start(rt.as_ref().offset() as u64))?;
    file.read_exact(&mut read_buf)?;
    // Read data is split across multiple "rows", so pull out the index
    // info from the ReadTable
    let read_ids = get_reads_info(&read_buf)?;

    let st = parsed.signal_table()?;
    let offset = st.as_ref().offset() as u64;
    let length = st.as_ref().length() as u64;

    let mut signal_buf = vec![0u8; length as usize];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut signal_buf)?;

    // Not sure how to get this working.
    // Ideally we could seek and just pass in the file to avoid allocation.
    // However, the arrow FileReader needs to seek to the end.
    // Something like Take should be able to limit the view into the file
    // but using the following has caused a version mismatch error.
    // let mut take = file.take(length);
    // take.read_to_end(&mut signal_buf)?;
    let cursor = Cursor::new(signal_buf);

    let reader = FileReader::try_new(cursor, None)?;
    // Print the schema to figure the columns and the array type
    println!("{}", reader.schema());

    for batch in reader {
        let batch = batch?;
        // Read ids are UUIDs represented by extension types
        // backed by FixedSizeBinaryArrays.
        let read_id_col = batch
            .column_by_name("read_id")
            .unwrap()
            .as_fixed_size_binary();
        println!("uuid col binary: {read_id_col:?}");

        // Notice there are duplicates, data for a read can be
        // split across multiple rows.
        for uuid_bin in read_id_col.into_iter() {
            let parsed_uuid = uuid::Uuid::from_slice(uuid_bin.unwrap()).unwrap();
            println!("uuid parsed: {}", parsed_uuid);
        }

        let samples_col: &PrimitiveArray<UInt32Type> =
            batch.column_by_name("samples").unwrap().as_primitive();
        println!("{samples_col:?}");

        let signal_col: &GenericByteArray<GenericBinaryType<i64>> =
            batch.column_by_name("signal").unwrap().as_binary();
        for read_idx in read_ids.iter() {
            let read_idx: &PrimitiveArray<UInt64Type> = read_idx.as_primitive();
            for rid in read_idx.iter().flatten() {
                let sigcol = signal_col.value(rid as usize);
                let samcol = samples_col.value(rid as usize);
                let decoded = svb16::decode(sigcol, samcol as usize);
                println!("len(signal) = {}", sigcol.len());
                println!("n = {samcol}");
                println!("Decode successful: {}", decoded.is_ok());
            }
        }
    }
    Ok(())
}
