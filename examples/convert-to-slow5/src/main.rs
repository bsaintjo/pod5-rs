use std::path::PathBuf;

use pico_args::Arguments;
use pod5;
use slow5;

fn run(path: PathBuf) -> eyre::Result<()> {
    todo!()
}

fn main() -> eyre::Result<()> {
    let mut args = Arguments::from_env();
    let path: PathBuf = args.free_from_str()?;
    run(path)?;
    Ok(())
}
