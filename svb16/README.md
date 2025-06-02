# POD5 compression & decompression algorithms in Rust

This crates focuses on the compression format used by Oxford Nanopore in FAST5/POD5 files.

## Installation

```bash
cargo add --git https://github.com/bsaintjo/pod5-rs svb16
```

## Generate Rust flatbuffer code

If the generated code needs to regenerated, here is how to run the flatbuffer compiler.

```bash
flatc -o src --rust footer.fbs
```

## Known issues

### Compressed output differs, but uncompressed output is the same

If you are trying validate this library and compare the compressed array from here to the official implementation, you will find the compressed arrays aren't exactly the same. However, from my preliminary tests, the decompressed output should match. I believe this issue is due to how the buffer is allocated, as this implementation iteratively builds up the compressed array, whereas the official implementation preallocates the array based on expectations on the size of the compressed output. If you find otherwise, or would like to help on this front, please open a GitHub issue.

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
