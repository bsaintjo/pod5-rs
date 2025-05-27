import pytest
import pod5frame as p5f
import polars as pl
import pod5


def test_read_fail() -> None:
    with pytest.raises(Exception):
        _ = p5f.FrameReader("NOTAREALFILE").reads()


def test_no_read_fail() -> None:
    with pytest.raises(Exception):
        reader = p5f.FrameReader(path="../extra/multi_fast5_zip_v3.pod5")
        _ = reader.signal()


def test_signal_iter():
    with p5f.FrameReader("../extra/multi_fast5_zip_v3.pod5") as reader:
        for sdf in reader.signal():
            aggregated = (
                sdf.group_by("read_id")
                .agg(pl.col("signal").explode(), pl.col("samples").sum())
                .with_columns(idx=pl.int_ranges(pl.col("signal").list.len()))
                .explode("signal", "idx")
            )
            print(aggregated)


def test_writer(tmp_path):
    with pytest.raises(Exception):
        with p5f.FrameWriter(tmp_path / "test.pod5") as writer:
            writer.write(p5f.TableType())
            writer.write(p5f.TableType())


def test_reader_writer_roundtrip(tmp_path):
    output = tmp_path / "test.pod5"
    with (
        p5f.FrameReader("../extra/multi_fast5_zip_v3.pod5") as reader,
        p5f.FrameWriter(output) as writer,
    ):
        writer.write_signal_tables(reader.signal())
        writer.write_read_tables(reader.reads())
        writer.write_run_info_tables(reader.run_info())
