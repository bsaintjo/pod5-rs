# pod5-rs

Experimental library for interacting with [POD5 files](https://github.com/nanoporetech/pod5-file-format) in Rust.

## Motivation: Dataframes for POD5 files

Major goals here are to explore combining POD5 files with `polars` DataFrame API. A POD5 file is essentially multiple Apache Arrow files stitched together using flatbuffers. Since `polars` has native support for Apache Arrow files, we should be able to treat the POD5 Arrow components as Dataframes and leverage all the API provided by `polars` to manipulate these.

This library performs the necessary casting in order to use `polars` on POD5 Apache Arrow contents. It provides a few convience functions for common operations, such as decompressing signal data, converting read ids into strings, etc. However, `polars` is re-exported to give full access to the DataFrame API.

## Example: Read out read IDs and convert to UUID

### Note: API is experimental and ex

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
            .drop("read_id")? // Drop the read since we don't need it anymore
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
- [ ] VBZ De/Compression
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

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
