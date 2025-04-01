# DataFrames for POD5 files

Explore POD5 files using DataFrames from polars. Not official.

This provides a Python module bindings to the `pod5` rust crate, so we can re-use those functions in Python.

## Installation

### Build wheel

Currently being built with Python 3.11 and Rust 1.85

```bash
# Activate a virtual environment with your favorite Python package manager, uv, poetry, venv, etc.
$ uv venv
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
pip install -r "git+https://github.com/bsaintjo/pod5-rs.git/#egg=pod5frame&subdirectory=pod5frame-py"
```

## Usage

### Reading a POD5 File

## Why use this package?

## Why not use this package?
