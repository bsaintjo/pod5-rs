# DataFrames for POD5 files in Python

Explore POD5 files using DataFrames from polars.

This provides a Python module bindings to the `pod5` rust crate, so we can re-use those functions in Python.

## Installation

### Build wheel locally

Currently being built with Python 3.11 and Rust 1.85

```bash
# Activate a virtual environment with your favorite Python package manager, uv, poetry, venv, etc.
$ source .venv/bin/activate

# Install maturin tool for building Python wheels from Rust crate
(pod5frame-py) $ pip install maturin

# Automatically build wheel and install into virtual environment
(pod5frame-py) $ maturin develop
📦 Built wheel for CPython 3.11 to /var/folders/s4/z4m2bhxj06g5pqdm6qgmq_bm0000gp/T/.tmphaRgIx/pod5frame-0.1.0-cp311-cp311-macosx_11_0_arm64.whl
✏️ Setting installed package as editable
🛠 Installed pod5frame-0.1.0
```

### `pip` via Git

Until the package gets into decent shape, I'll hold off on uploading to PyPI. If you are still interested, you can install via git with this command

```bash
git clone https://github.com/bsaintjo/pod5-rs.git
cd pod5-rs/pod5frame-py
pip install -e .
```

## Usage

### Reading a POD5 File

The API is setup to mirror the official Python API for easy of use.

```python
import pod5frame as p5f

with p5f.FrameReader("example.pod5") as reader:
    for signal_df in reader.signal():
        aggregated = signal_df.group_by("read_id").agg(
            pl.col("signal").explode(), pl.col("samples").sum()
        )
        print(aggregated)
```

Output

```text
┌─────────────────────────────────┬───────────────────┬─────────┐
│ read_id                         ┆ signal            ┆ samples │
│ ---                             ┆ ---               ┆ ---     │
│ str                             ┆ list[i16]         ┆ u32     │
╞═════════════════════════════════╪═══════════════════╪═════════╡
│ 00925f34-6baf-47fc-b40c-22591e… ┆ [434, 328, … 366] ┆ 136370  │
│ 008468c3-e477-46c4-a6e2-7d021a… ┆ [530, 531, … 523] ┆ 206976  │
│ 00919556-e519-4960-8aa5-c2dfa0… ┆ [531, 520, … 416] ┆ 9885    │
│ 002fde30-9e23-4125-9eae-d112c1… ┆ [632, 381, … 373] ┆ 37440   │
│ 009dc9bd-c5f4-487b-ba4c-b9ce7e… ┆ [546, 332, … 286] ┆ 15643   │
│ 008ed3dc-86c2-452f-b107-6877a4… ┆ [583, 306, … 338] ┆ 14510   │
│ 007cc97e-6de2-4ff6-a0fd-1c1eca… ┆ [679, 419, … 433] ┆ 505057  │
│ 0000173c-bf67-44e7-9a9c-1ad0bc… ┆ [371, 370, … 313] ┆ 123627  │
│ 006d1319-2877-4b34-85df-34de72… ┆ [571, 387, … 710] ┆ 337876  │
│ 00728efb-2120-4224-87d8-580fbb… ┆ [431, 440, … 507] ┆ 161547  │
└─────────────────────────────────┴───────────────────┴─────────┘
```

## Why use this package?

- If you are familiar with polars/DataFrame API, this package makes it easy to pull out any POD5 data into a polars DataFrame.

## Why not use this package?

- Experimental
- Polars tends to be more suited to operations over columns rather than rows. If you

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
