use std::{
    fs::File,
    path::{Path, PathBuf},
};

use pico_args::Arguments;
use pod5::{self, reader::Reader};

fn run(path: PathBuf) -> eyre::Result<()> {
    let file = File::open(path)?;
    let mut reader = Reader::from_reader(file)?;
    let read_iter = reader.read_dfs()?;
    for read_df in read_iter.flatten() {
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

fn main() -> eyre::Result<()> {
    let mut args = Arguments::from_env();
    let path: PathBuf = args.free_from_str()?;
    run(path)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_find_read_ids() -> eyre::Result<()> {
        let path = PathBuf::from("../../extra/multi_fast5_zip_v0.pod5");
        println!("{path:?}");
        run(path)?;

        let path = PathBuf::from("../../extra/multi_fast5_zip_v1.pod5");
        println!("{path:?}");
        run(path)?;

        let path = PathBuf::from("../../extra/multi_fast5_zip_v2.pod5");
        println!("{path:?}");
        run(path)?;

        let path = PathBuf::from("../../extra/multi_fast5_zip_v3.pod5");
        println!("{path:?}");
        run(path)?;
        Ok(())
    }
}