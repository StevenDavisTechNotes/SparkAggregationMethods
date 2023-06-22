from typing import List, Callable, Tuple, Optional
from dataclasses import dataclass

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from .BiLevelTestData import DataPoint
from .Strategy.BiLevelSqlJoin import bi_sql_join
from .Strategy.BiLevelFluentJoin import bi_fluent_join
from .Strategy.BiLevelPandas import bi_pandas
from .Strategy.BiLevelPandasNumba import bi_pandas_numba
from .Strategy.BiLevelSqlNested import bi_sql_nested
from .Strategy.BiLevelFluentNested import bi_fluent_nested
from .Strategy.BiLevelFluentWindow import bi_fluent_window
from .Strategy.BiLevelRddGrpMap import bi_rdd_grpmap
from .Strategy.BiLevelRddReduce1 import bi_rdd_reduce1
from .Strategy.BiLevelRddReduce2 import bi_rdd_reduce2
from .Strategy.BiLevelRddMapPart import bi_rdd_mappart


@dataclass(frozen=True)
class PythonTestMethod:
    name: str
    language: str
    interface: str
    delegate: Callable[
        [TidySparkSession, List[DataPoint]],
        Tuple[Optional[RDD], Optional[spark_DataFrame]]]


implementation_list: List[PythonTestMethod] = [
    PythonTestMethod(
        name='bi_sql_join',
        language='python',
        interface='sql',
        delegate=bi_sql_join
    ),
    PythonTestMethod(
        name='bi_fluent_join',
        language='python',
        interface='fluent',
        delegate=bi_fluent_join
    ),
    PythonTestMethod(
        name='bi_pandas',
        language='python',
        interface='pandas',
        delegate=bi_pandas
    ),
    PythonTestMethod(
        name='bi_pandas_numba',
        language='python',
        interface='pandas',
        delegate=bi_pandas_numba
    ),
    PythonTestMethod(
        name='bi_sql_nested',
        language='python',
        interface='sql',
        delegate=bi_sql_nested
    ),
    PythonTestMethod(
        name='bi_fluent_nested',
        language='python',
        interface='fluent',
        delegate=bi_fluent_nested
    ),
    PythonTestMethod(
        name='bi_fluent_window',
        language='python',
        interface='fluent',
        delegate=bi_fluent_window
    ),
    PythonTestMethod(
        name='bi_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=bi_rdd_grpmap
    ),
    PythonTestMethod(
        name='bi_rdd_reduce1',
        language='python',
        interface='rdd',
        delegate=bi_rdd_reduce1
    ),
    PythonTestMethod(
        name='bi_rdd_reduce2',
        language='python',
        interface='rdd',
        delegate=bi_rdd_reduce2
    ),
    PythonTestMethod(
        name='bi_rdd_mappart',
        language='python',
        interface='xxxxxxx',
        delegate=bi_rdd_mappart
    ),
]


strategy_name_list = [x.name for x in implementation_list]
