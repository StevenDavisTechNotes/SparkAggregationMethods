from typing import List, Callable, Tuple, Any, cast, Optional, Iterable
from dataclasses import dataclass

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

@dataclass(frozen=True)
class PythonTestMethod:
    name: str
    language: str
    interface: str
    delegate: Callable[
        [TidySparkSession, List],
        Tuple[Optional[RDD], Optional[spark_DataFrame]]]


@dataclass(frozen=True)
class ExternalTestMethod:
    name: str
    language: str
    interface: str


@dataclass(frozen=True)
class FullCondMethod:
    data_name: str
    raw_method_name: str
    language: str
    interface: str

def count_iter(iterator: Iterable):
    count = 0
    for _ in iterator:
        count += 1
    return count
