use std::{error::Error, fs::File, path::PathBuf};

use eyre::eyre;
use if_chain::if_chain;
use pico_args::Arguments;
use pod5::{self, polars::prelude::AnyValue, reader::Reader};
use slow5::{self, FileWriter};

fn run(path: PathBuf, output: PathBuf) -> eyre::Result<()> {
    let file = File::open(path)?;
    let output = FileWriter::create(output)?;

    let mut reader = Reader::from_reader(file)?;
    let mut read_iter = reader.read_dfs()?;
    let next = read_iter.next().ok_or(eyre::eyre!("Reading went bad"))??;
    let binding = next.into_inner();
    println!("{binding:?}");

    #[allow(clippy::never_loop)]
    for row_idx in 0..binding.height() {
        if let Some(row) = binding.get(row_idx) {
            println!("{row:?}");
            if_chain! {
                if let AnyValue::Binary(row_id) = row[0];
                then {
                    dbg!("row_id: {row_id:?}");
                } else {
                    return Err(eyre!("id"));
                }
            }
        }
        break;
    }
    // for x in next.into_inner().iter_chunks(false) {
    //     println!("{x:?}");
    // }
    Ok(())
}

fn main() -> eyre::Result<()> {
    let mut args = Arguments::from_env();
    let input: PathBuf = args.free_from_str()?;
    let output: PathBuf = args.free_from_str()?;
    run(input, output)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::env::temp_dir;

    use super::*;

    #[test]
    fn test_conversion() {
        let path = PathBuf::from("../../extra/multi_fast5_zip_v3.pod5");
        let output = temp_dir().join("output.slow5");
        if let Err(e) = run(path, output) {
            println!("{e:?}");
        }
    }
}
