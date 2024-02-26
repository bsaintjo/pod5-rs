# pod5-rs

Experimental library for interacting with [POD5 files](https://github.com/nanoporetech/pod5-file-format) in Rust.

## Motivation: Dataframes for POD5 files

Major goals here are to explore combining POD5 files with `polars` DataFrame API. A POD5 file is essentially multiple Apache Arrow files stitched together using flatbuffers. Since `polars` has native support for Apache Arrow files, we should be able to treat the POD5 Arrow components as Dataframes and leverage all the API provided by `polars` to manipulate these.

This library performs the necessary casting in order to use `polars` on POD5 Apache Arrow contents. It provides a few convience functions for common operations, such as decompressing signal data, converting read ids into strings, etc. However, `polars` is re-exported to give full access to the DataFrame API.

## Roadmap

- [x] VBZ Compression
- [ ] DataFrame for Signal Table
  - [x] Convert read_id into binary
  - [x] Decompress signal data
- [ ] DataFrame for Run Info Table
- [ ] DataFrame for Reads Table
- [ ] Support mmap
- [ ] Arrow-based API

## Notes

I won't be working on this consistently, so here are a shmattering of notes for anyone who might be interested in how I got this working.

in [`svb16.rs`](src/svb16.rs), there is an implementation of the code for decompressing compressed POD5
In short, ONT uses a different version of streamvbyte to deal with 16-bit integers (signal output) instead of 32-bit like the original