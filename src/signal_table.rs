use std::io::Cursor;

use polars::{frame::DataFrame, series::Series};
use polars_arrow::io::ipc::read::FileReader;

struct SignalTableDataFrame {
    df: DataFrame
}

impl SignalTableDataFrame {
    fn new(df: DataFrame) -> Self {
        Self { df }
    }

    fn into_inner(self) -> DataFrame {
        self.df
    }

    fn read_id(&self) -> Series {
        todo!()
    }

    fn read_signals(&self, ids: &[usize]) -> Vec<f64> {
        todo!()
    }
}