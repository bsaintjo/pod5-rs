use std::{env, fs::File, path::PathBuf};

use pod5::{
    dataframe::{ReadDataFrame, RunInfoDataFrame, SignalDataFrame},
    reader::Reader,
    writer::Writer,
};

fn main() {
    env_logger::init();

    let mut args = env::args();
    let _ = args.next();
    let input = args.next().unwrap();
    let output = args.next().unwrap();

    let input = PathBuf::from(input);
    let output = PathBuf::from(output);

    let input_file = File::open(input).unwrap();
    let output_file = File::create(output).unwrap();

    let mut reader = Reader::from_reader(input_file).unwrap();
    let mut writer = Writer::from_writer(output_file).unwrap();

    let mut guard = writer.guard::<SignalDataFrame>();
    for df in reader.signal_dfs().unwrap() {
        let df = df.unwrap();
        guard.write_batch(&df).unwrap();
    }
    guard.finish().unwrap();

    let mut guard = writer.guard::<ReadDataFrame>();
    for df in reader.read_dfs().unwrap() {
        let df = df.unwrap();
        guard.write_batch(&df).unwrap();
    }
    guard.finish().unwrap();

    let mut guard = writer.guard::<RunInfoDataFrame>();
    for df in reader.run_info_dfs().unwrap() {
        let df = df.unwrap();
        guard.write_batch(&df).unwrap();
    }
    guard.finish().unwrap();

    writer.finish().unwrap();
}
