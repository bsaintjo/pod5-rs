use std::path::PathBuf;

use pod5::polars::df;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

#[pyclass]
struct FrameReader {
    path: PathBuf
}

#[pymethods]
impl FrameReader {

    #[new]
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
    
    fn __enter__(me: PyRef<'_, Self>) {
        todo!()
    }

    fn __exit__(me: PyRef<'_, Self>) {
        todo!()
    }

    fn reads(&self) {
        todo!()
    }

    /// Return an iterator over signal data represented as polars DataFrames
    fn signal(me: PyRef<'_, Self>) {
        todo!()
    }

    fn run_info(me: PyRef<'_, Self>) {
        todo!()
    }
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
/// Test comment
fn load(path: PathBuf) -> PyResult<PyDataFrame> {
    Ok(PyDataFrame(
        df!("Fruit" => ["Apple"], "Color" => ["Red"]).unwrap(),
    ))
}

/// A Python module implemented in Rust.
#[pymodule]
fn pod5frame(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(load, m)?)?;
    m.add_class::<FrameReader>()?;
    Ok(())
}
