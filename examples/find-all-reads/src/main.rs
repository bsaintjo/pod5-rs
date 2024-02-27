use std::{
    fs::File,
    path::{Path, PathBuf},
};

use pico_args::Arguments;
use pod5::{self, reader::Reader};

fn main() -> eyre::Result<()> {
    let mut args = Arguments::from_env();
    let path: PathBuf = args.free_from_str()?;
    let file = File::open(path)?;
    let mut reader = Reader::from_reader(file)?;
    for read_df in reader.read_dfs()?.flatten() {
        for read_id in read_df
            .parse_read_ids("uuid")?
            .into_inner()
            .column("uuid")?
            .str()?
            .into_iter()
            .flatten() // Skip null rows
        {
            println!("{read_id:?}");
        }
    }
    Ok(())
}
