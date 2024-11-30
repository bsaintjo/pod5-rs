use std::{
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom},
};

use arrow::{array::AsArray, ipc::reader::FileReader};
use pod5::footer::ParsedFooter;

fn main() -> eyre::Result<()> {
    let path = "extra/multi_fast5_zip_v3.pod5";
    let mut file = File::open(path)?;
    let parsed = ParsedFooter::read_footer(&file)?;
    let footer = parsed.footer()?;

    println!("footer: {footer:?}");

    let embedded = footer.contents().unwrap();
    let efile = embedded.get(0);
    let offset = efile.offset() as u64;
    let length = efile.length() as u64;

    let mut signal_buf = vec![0u8; length as usize];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut signal_buf)?;

    // let mut take = file.take(length);
    // take.read_to_end(&mut signal_buf)?;
    let cursor = Cursor::new(signal_buf);

    let reader = FileReader::try_new(cursor, None)?;
    // Print the schema to figure the columns and the array type
    println!("{}", reader.schema());

    for batch in reader {
        let batch = batch?;
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
    }
    Ok(())
}
