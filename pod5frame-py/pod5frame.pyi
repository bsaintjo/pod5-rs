from typing import Iterable

from polars import DataFrame

class FrameReader:
    """
    A class representing a reader over a POD5 file.

    :param path: File path to POD5 file
    """
    def __init__(self, path: str) -> None: ...

    # @classmethod
    # def from_unique_name(cls, name: str) -> 'FrameReader':
    #     """
    #     Creates a Car based on unique name

    #     :param name: model name of a car to be created
    #     :return: a Car instance with default data
    #     """

    def reads(self) -> Iterable[DataFrame]: ...
    """
    Iterate over ReadTable dataframes from a POD5 file

    :return: Iterable of polars DataFrames with for the ReadTable
    """

    def signal(self) -> Iterable[DataFrame]: ...
    """
    Iterate over SignalTable dataframes from a POD5 file

    :return: Iterable of polars DataFrames with for the ReadTable
    """

    def close(self): ...
    """Close the file, subsequent read method calls will fail."""

class FrameWriter:
    """
    A class for writing dataframes to POD5 files

    :param path: File path to POD5 file
    """
    def __init__(self, path: str) -> None: ...
    def write_signal_tables(tables: Iterable[DataFrame]): ...
    """
    Write signal dataframes to the POD5 file.o

    :param tables: Iterable of signal dataframes, with samples, read_id, and signal column
    """

    def close(): ...
    """
    Close the writer. Any subsequent write method calls will fail
    """
