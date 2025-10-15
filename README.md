# pod5-rs

[![stable][stability-badge]][stability-url]

[stability-badge]: https://img.shields.io/badge/stability-experimental-orange.svg
[stability-url]: https://github.com/mkenney/software-guides/blob/master/STABILITY-BADGES.md#experimental

Experimental library for interacting with [POD5 files](https://github.com/nanoporetech/pod5-file-format) using the Rust programming language. This repository provides multiple crates covering different portions of the POD5 file format and APIs for reading and writing.

## Getting Started

### Example with the `pod5-polars` integration

```rust

use std::{fs::File, path::PathBuf, error::Error};
use pod5_polars::{self, reader::Reader};

fn run() -> Result<(), Box<dyn Error>> {
    let path = "../extra/multi_fast5_zip_v3.pod5";
    let file = File::open(path)?;
    let mut reader = Reader::from_reader(file)?;
    let calibration = reader.read_dfs()?.into_calibration();
    let signal_df = reader.signal_dfs()?.flatten().next().unwrap();
    println!("{:?}", signal_df.to_picoamps(&calibration));
    Ok(())
}
```

Output:

```text
SignalDataFrame(shape: (22, 3)
┌─────────────────────────────────┬─────────────────────────────────┬─────────┐
│ read_id                         ┆ signal                          ┆ samples │
│ ---                             ┆ ---                             ┆ ---     │
│ str                             ┆ list[f32]                       ┆ u32     │
╞═════════════════════════════════╪═════════════════════════════════╪═════════╡
│ 0000173c-bf67-44e7-9a9c-1ad0bc… ┆ [68.796082, 68.620575, … 64.75… ┆ 102400  │
│ 0000173c-bf67-44e7-9a9c-1ad0bc… ┆ [65.110573, 74.587585, … 58.61… ┆ 21227   │
│ 002fde30-9e23-4125-9eae-d112c1… ┆ [111.618126, 67.567574, … 66.1… ┆ 37440   │
│ 006d1319-2877-4b34-85df-34de72… ┆ [101.263618, 68.971581, … 70.2… ┆ 102400  │
│ 006d1319-2877-4b34-85df-34de72… ┆ [73.359085, 69.673576, … 66.33… ┆ 102400  │
│ …                               ┆ …                               ┆ …       │
│ 008ed3dc-86c2-452f-b107-6877a4… ┆ [103.194115, 54.580563, … 60.1… ┆ 14510   │
│ 00919556-e519-4960-8aa5-c2dfa0… ┆ [93.541603, 91.611107, … 73.35… ┆ 9885    │
│ 00925f34-6baf-47fc-b40c-22591e… ┆ [82.660591, 64.057571, … 72.30… ┆ 102400  │
│ 00925f34-6baf-47fc-b40c-22591e… ┆ [60.723068, 64.057571, … 70.72… ┆ 33970   │
│ 009dc9bd-c5f4-487b-ba4c-b9ce7e… ┆ [98.280113, 60.723068, … 52.65… ┆ 15643   │
└─────────────────────────────────┴─────────────────────────────────┴─────────┘)
```

## Installation

The crates will eventually be added to `crates.io`. To use any of these crates now, you can use `cargo`'s git parameter to add any of the subcrates to your project. To add the `pod5-polars` to do something similar in the example above, run:

```bash
cargo add --git https://github.com/bsaintjo/pod5-rs pod5-polars
```

Change `pod5` to any of the crates mentioned in the [Crates](#crates) section for other access.

## Crates

### `pod5-polars`

Reading and writing of POD5 files with integration with `polars` and its DataFrame API.

### `pod5frame`

Exports the Polars POD5 DataFrame API as a Python package. To use this in Python and install in a Python virtual environment, please read the [Python installation instructions](./pod5frame-py/README.md#installation).

### `pod5-format`

Crate for dealing with non-internal file format details, including file signature and the FlatBuffers footer. Useful for writing and reusing the code with other Arrow handling library or implementations.

### `pod5-arrow`

Experimental implementation for using reading/writing POD5 files with the official Rust Apache Arrow crate. Maybe useful if you are interested in implementing your own POD5 reader/writer and want to see how to use some of the components in this workspace.

### `svb16`

This crate allows for encoding and decoding of nanopore signal data in FAST5/POD5 which are usually compressed in a custom schema.

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
