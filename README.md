# pod5-rs

Experimental library for interacting with [POD5 files](https://github.com/nanoporetech/pod5-file-format) in rust.

Major goals here are to explore combining POD5 files with `polars` DataFrame API since are POD5 files consist of and 

## Notes

polars doesn't support FixedSizeBinary or Extensions directly
Extensions can get converted automatically, but need to cast FixedSizeBinary
[List of supported Arrow types](https://docs.rs/polars/latest/polars/datatypes/enum.AnyValue.html#variants)

in [`svb16.rs`](src/svb16.rs), there is an implementation of the code for decompressing compressed POD5
In short, ONT uses a different version of streamvbyte to deal with 16-bit integers (signal output) instead of 32-bit like the original