import pytest
import pod5frame as p5f
import polars as pl


def test_read_fail() -> None:
    with pytest.raises(Exception):
        p5f.FrameReader("NOTAREALFILE").read()


def test_no_read_fail() -> None:
    with pytest.raises(Exception):
        reader = p5f.FrameReader(path="../extra/multi_fast5_zip_v3.pod5")
        _ = reader.signal()


def test_test():
    assert p5f.sum_as_string(1, 2) == "3"


def test_signal_iter():
    with p5f.FrameReader("../extra/multi_fast5_zip_v3.pod5") as reader:
        for sdf in reader.signal():
            aggregated = (
                sdf.group_by("minknow.uuid")
                .agg(pl.col("minknow.vbz").explode(), pl.col("samples").sum())
                .with_columns(idx=pl.int_ranges(pl.col("minknow.vbz").list.len()))
                .explode("minknow.vbz", "idx")
            )
            print(aggregated)


def test_writer():
    with pytest.raises(Exception):
        with p5f.FrameWriter("test.pod5") as writer:
            writer.write(p5f.TableType())
            writer.write(p5f.TableType())
