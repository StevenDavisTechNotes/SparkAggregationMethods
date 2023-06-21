from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from DedupePerfTest.DedupeTestData import DedupeDataParameters
from Utils.SparkUtils import TidySparkSession

from .Strategy.DedupeFluentNestedPandas import method_pandas
from .Strategy.DedupeFluentNestedPython import method_fluent_nested_python
from .Strategy.DedupeFluentNestedWCol import method_fluent_nested_withCol
from .Strategy.DedupeFluentWindow import method_fluent_windows
from .Strategy.DedupeRddGroupBy import method_rdd_groupby
from .Strategy.DedupeRddMapPart import method_rdd_mappart
from .Strategy.DedupeRddReduce import method_rdd_reduce


@dataclass(frozen=True)
class PythonTestMethod:
    name: str
    language: str
    interface: str
    delegate: Callable[
        [TidySparkSession, DedupeDataParameters, int, spark_DataFrame],
        Tuple[Optional[RDD], Optional[spark_DataFrame]]]


implementation_list: List[PythonTestMethod] = [
    PythonTestMethod(
        name='method_pandas',
        language='python',
        interface='pandas',
        delegate=method_pandas,
    ),
    PythonTestMethod(
        name='method_fluent_nested_python',
        language='python',
        interface='fluent',
        delegate=method_fluent_nested_python,
    ),
    PythonTestMethod(
        name='method_fluent_nested_withCol',
        language='python',
        interface='fluent',
        delegate=method_fluent_nested_withCol,
    ),
    PythonTestMethod(
        name='method_fluent_windows',
        language='python',
        interface='fluent',
        delegate=method_fluent_windows,
    ),
    PythonTestMethod(
        name='method_rdd_groupby',
        language='python',
        interface='rdd',
        delegate=method_rdd_groupby,
    ),
    PythonTestMethod(
        name='method_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=method_rdd_mappart,
    ),
    PythonTestMethod(
        name='method_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=method_rdd_reduce,
    ),
]
strategy_name_list = [x.name for x in implementation_list]