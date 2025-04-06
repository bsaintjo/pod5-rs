/// Explore an API using Box<dyn Fn> for conversion
use polars::{
    prelude::ArrowField,
    series::Series,
};
use polars_arrow::array::Array;
use polars::prelude as pl;

use crate::array_to_series;

struct X(Box<dyn Fn() -> Series>);

trait SeriesToArrow {}

struct Foo;

impl<F> SeriesToArrow for F where F: Fn(&ArrowField, Box<dyn Array>) -> Series {}
impl SeriesToArrow for Foo {}

#[derive(Default)]
struct SeriesConverters(Vec<Box<dyn SeriesToArrow>>);

impl SeriesConverters {
    fn new() -> Self {
        SeriesConverters(vec![Box::new(array_to_series)])
    }
    fn with_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&ArrowField, Box<dyn Array>) -> Series + 'static,
    {
        self.0.push(Box::new(f));
        self
    }
}

pub(crate) fn array_to_series2(
    field: &pl::ArrowField,
    arr: Box<dyn Array>,
    converters: SeriesConverters,
) -> Series {
    todo!()
}
