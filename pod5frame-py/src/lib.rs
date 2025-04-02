use std::{fs::File, path::PathBuf};

use pod5::{polars::df, reader::Reader};
use pyo3::{
    exceptions::{PyException, PyIOError},
    prelude::*,
};
use pyo3_polars::PyDataFrame;

/// An iterator over the SignalTable, yielding polars DataFrames
#[pyclass]
struct SignalIter(pod5::dataframe::SignalDataFrameIter);

#[pymethods]
impl SignalIter {
    fn __iter__(me: PyRef<'_, Self>) -> PyRef<'_, Self> {
        me
    }

    fn __next__(mut me: PyRefMut<'_, Self>) -> Option<PyDataFrame> {
        match me.0.next() {
            Some(Ok(x)) => Some(PyDataFrame(x.into_inner())),
            _ => None,
        }
    }
}

/// Reads a POD5 file and allows for getting various iterators over parts of the file
#[pyclass]
struct FrameReader {
    path: PathBuf,
    reader: Option<Reader<File>>,
}

#[pymethods]
impl FrameReader {
    #[new]
    fn new(path: PathBuf) -> PyResult<Self> {
        Ok(Self { path, reader: None })
    }

    fn read(&mut self) -> PyResult<()> {
        let file = File::open(&self.path)?;
        self.reader = Some(
            Reader::from_reader(file).map_err(|_| PyIOError::new_err("Failed to read file."))?,
        );
        Ok(())
    }

    fn close(&mut self) {
        self.reader = None;
    }

    fn __enter__(mut me: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        me.read()?;
        Ok(me)
    }

    fn __exit__(&mut self, _exc_type: PyObject, _exc_val: PyObject, _exc_tb: PyObject) {
        self.close()
    }

    fn reads(&self) {
        todo!()
    }

    /// Return an iterator over signal data represented as polars DataFrames
    fn signal(&mut self) -> PyResult<SignalIter> {
        self.reader
            .as_mut()
            .ok_or_else(|| PyException::new_err("Must call read method or use context manager"))?
            .signal_dfs()
            .map(SignalIter)
            .map_err(|_| PyException::new_err("Missing SignalTable"))
    }

    fn run_info(&self) {
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
fn load(_path: PathBuf) -> PyResult<PyDataFrame> {
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
    utils(m)?;
    Ok(())
}

fn utils(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let utils_module = PyModule::new(m.py(), "utils")?;
    m.add_submodule(&utils_module)?;
    Ok(())
}
