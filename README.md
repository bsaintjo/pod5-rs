# pod5-rs

[![stable][stability-badge]][stability-url]

[stability-badge]: https://img.shields.io/badge/stability-experimental-orange.svg
[stability-url]: https://github.com/mkenney/software-guides/blob/master/STABILITY-BADGES.md#experimental

Experimental library for interacting with [POD5 files](https://github.com/nanoporetech/pod5-file-format). While this repository has focused on reading POD5 files using Rust and `polars`, there are `arrow`-only and Python bindings available as well.

## Installation

To use this crate in your project, run:

```bash
cargo add --git https://github.com/bsaintjo/pod5-rs
```

### Python version

If you are interested in the Python package, checkout [here](./pod5frame-py/README.md).

## Motivation: Dataframes for POD5 files

Major goals here are to explore combining POD5 files with `polars` DataFrame API. A POD5 file is essentially multiple Apache Arrow files stitched together using flatbuffers. Since `polars` has native support for Apache Arrow files, we should be able to treat the POD5 Arrow components as Dataframes and leverage all the API provided by `polars` to manipulate these.

This library performs the necessary casting in order to use `polars` on POD5 Apache Arrow contents. It provides a few convience functions for common operations, such as decompressing signal data, converting read ids into strings, etc. However, `polars` is re-exported to give full access to the DataFrame API.

## Example: Print read IDs, signal data, and number of samples

### Note: API is experimental and expect breaking changes. If there you are interested in using or having additional features, feel free to contact

```rust

use std::{fs::File, path::PathBuf, error::Error};
use pod5::{self, reader::Reader};

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
│ minknow.uuid                    ┆ minknow.vbz                     ┆ samples │
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

## Why use this

- Use this if you are familiar with DataFrame APIs (pandas, polars, etc.)
- Want to export POD5 into different formats
  - Since we export the full polars API, its relatively straightforward to output your DataFrame as JSON or serde supported formats

## Why not use this

- You aren't familiar with details of POD5 data format
  - For example, in the above example, data from the same read is split across multiple rows. If you want to iterate over the signal data for each read, you will need to use additional Dataframe methods like polars, roughly `.groupby(["read_id"]).agg([col("samples").sum(), col("minknow.vbz").explode()])` to an aggregated DataFrame.
  - To know what data you are want, you need to know which table contains your data and what the is the name of the column(s) you are interested in. For more documentation on what information each table, check the [TOML files here](https://github.com/nanoporetech/pod5-file-format/tree/master/docs/tables) for column names, data types, and documentation.
  - Common columns:
    - Signal data: SignalTable minknow.vbz
    - Read ID: read_id in each table
- You need to integrate data across multiple tables
  - For storage efficiency, certain information is split into different POD5 tables. To combine these tables you need to intersect the two tables based on some index. For example, if you want to filter reads based on the end reason, you need to pull that information from the ReadTable, then
- You need to write POD5 files
  - Currently only support is available for reading
- You need support for older versions of POD5 files
  - API is based on POD5 v3, and it may work with other versions but likely will panic.

## Roadmap

- [ ] Python integration via maturin and PyO3
- [ ] Support for read indexing
- [ ] Convenience/examples for conversion to SLOW5/BLOW5
- [ ] VBZ de/compression
  - [x] Decompression
  - [ ] Compression (works but isn't exact?)
- [ ] DataFrame for Signal Table
  - [x] Convert read_id into binary
  - [x] Decompress signal data
- [ ] DataFrame for Run Info Table
- [ ] DataFrame for Reads Table
- [ ] Support mmap
- [ ] Arrow-based API
- [ ] Optimize decompression
  - [ ] Switch Zig-zag encoding dependency
  - [ ] Try `varint-rs`, `varint-simd`, etc.

## Known issues

### Compressed output differs, but uncompressed output is the same

If you are trying validate this library and compare the compressed array from here to the official implementation, you will find the arrays aren't exactly the same. However, from my preliminary tests, the decompressed output should match. I believe this issue is due to how the buffer is allocated, as this implementation iteratively builds up the compressed array, whereas the official implementation preallocates the array based on expectations on the size of the compressed output. If you find otherwise, or would like to help on this front, please open a GitHub issue.

### Dataframes can drop certain types of columns

Certain types of columns in the a POD5 Arrow file, such as map arrays, are not supported by polars at this point. For now, some of these columns are dropped and logging will inform when these occur. There are some ways around this, and if there is a column(s) you'd like for the DataFrame API to be able to support, please open a GitHub issue.

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
