# pod5 crate

## Installation

```bash
cargo add --git https://github.com/bsaintjo/pod5-rs pod5
```

## Motivation: Dataframes for POD5 files

Major goals here are to explore combining POD5 files with `polars` DataFrame API. A POD5 file is essentially multiple Apache Arrow files stitched together using flatbuffers. Since `polars` has native support for Apache Arrow files, we should be able to treat the POD5 Arrow components as Dataframes and leverage all the API provided by `polars` to manipulate these.

This library performs the necessary casting in order to use `polars` on POD5 Apache Arrow contents. It provides a few convience functions for common operations, such as decompressing signal data, converting read ids into strings, etc. However, `polars` is re-exported to give full access to the DataFrame API.

## Why use this

- Use this if you are familiar with DataFrame APIs (pandas, polars, etc.)
- Want to export POD5 into different formats
  - Since we export the full polars API, its relatively straightforward to output your DataFrame as JSON or serde supported formats

## Why not use this

- This crate is a work in progress. If there is a feature you are interested in, please open a GitHub issue.

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
