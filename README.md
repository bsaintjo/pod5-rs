# pod5-rs

[![stable][stability-badge]][stability-url]

[stability-badge]: https://img.shields.io/badge/stability-experimental-orange.svg
[stability-url]: https://github.com/mkenney/software-guides/blob/master/STABILITY-BADGES.md#experimental

Experimental library for interacting with [POD5 files](https://github.com/nanoporetech/pod5-file-format) in Rust.

## Installation

To use this crate in your project, run:

```bash
cargo add --git https://github.com/bsaintjo/pod5-rs
```

## Motivation: Dataframes for POD5 files

Major goals here are to explore combining POD5 files with `polars` DataFrame API. A POD5 file is essentially multiple Apache Arrow files stitched together using flatbuffers. Since `polars` has native support for Apache Arrow files, we should be able to treat the POD5 Arrow components as Dataframes and leverage all the API provided by `polars` to manipulate these.

This library performs the necessary casting in order to use `polars` on POD5 Apache Arrow contents. It provides a few convience functions for common operations, such as decompressing signal data, converting read ids into strings, etc. However, `polars` is re-exported to give full access to the DataFrame API.

## Example: Read out read IDs and convert to UUID

### Note: API is experimental and expect breaking changes. If there you are interested in using or having additional features, feel free to contact

```rust

use std::{fs::File, path::PathBuf, error::Error};
use pod5::{self, reader::Reader};

fn run() -> Result<(), Box<dyn Error>> {
    let path = "../extra/multi_fast5_zip_v3.pod5";
    let file = File::open(path)?;
    let mut reader = Reader::from_reader(file)?;
    for read_df in reader.read_dfs()?.flatten() {
        let df = read_df
            .parse_read_ids("uuid")? // Convience method to parse read ID bytes into UUID
            .into_inner() // Extract inner polars DatFrame, get full access to API
            .drop("read_id")? // Drop the column since we already parsed it
            .head(Some(4)); // Limit output to fit into example
        println!("{df:?}");
    }
    Ok(())
}
```

Output:

```text
shape: (4, 21)
┌─────────────┬─────────────┬─────────┬───────────────┬───┬────────────┬───────────────────┬─────────────────────────────────┬─────────────────────────────────┐
│ signal      ┆ read_number ┆ start   ┆ median_before ┆ … ┆ end_reason ┆ end_reason_forced ┆ run_info                        ┆ uuid                            │
│ ---         ┆ ---         ┆ ---     ┆ ---           ┆   ┆ ---        ┆ ---               ┆ ---                             ┆ ---                             │
│ list[u64]   ┆ u32         ┆ u64     ┆ f32           ┆   ┆ cat        ┆ bool              ┆ cat                             ┆ str                             │
╞═════════════╪═════════════╪═════════╪═══════════════╪═══╪════════════╪═══════════════════╪═════════════════════════════════╪═════════════════════════════════╡
│ [0, 1]      ┆ 1093        ┆ 4534321 ┆ 183.107742    ┆ … ┆ unknown    ┆ false             ┆ a08e850aaa44c8b56765eee10b386f… ┆ 0000173c-bf67-44e7-9a9c-1ad0bc… │
│ [2]         ┆ 75          ┆ 122095  ┆ 174.630371    ┆ … ┆ unknown    ┆ false             ┆ a08e850aaa44c8b56765eee10b386f… ┆ 002fde30-9e23-4125-9eae-d112c1… │
│ [3, 4, … 6] ┆ 1053        ┆ 4347870 ┆ 193.573578    ┆ … ┆ unknown    ┆ false             ┆ a08e850aaa44c8b56765eee10b386f… ┆ 006d1319-2877-4b34-85df-34de72… │
│ [7, 8]      ┆ 657         ┆ 7231572 ┆ 194.65097     ┆ … ┆ unknown    ┆ false             ┆ a08e850aaa44c8b56765eee10b386f… ┆ 00728efb-2120-4224-87d8-580fbb… │
└─────────────┴─────────────┴─────────┴───────────────┴───┴────────────┴───────────────────┴─────────────────────────────────┴─────────────────────────────────┘
```

## Roadmap

- [ ] Convienence/examples for conversion to SLOW5/BLOW5
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
