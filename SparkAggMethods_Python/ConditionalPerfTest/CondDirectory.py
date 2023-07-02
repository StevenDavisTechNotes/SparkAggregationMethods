from dataclasses import dataclass
from typing import Callable, List, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from .CondTestData import DataPoint
from .Strategy.CondFluentJoin import cond_fluent_join
from .Strategy.CondFluentNested import cond_fluent_nested
from .Strategy.CondFluentNull import cond_fluent_null
from .Strategy.CondFluentWindow import cond_fluent_window
from .Strategy.CondFluentZero import cond_fluent_zero
from .Strategy.CondPandas import cond_pandas
from .Strategy.CondPandasNumba import cond_pandas_numba
from .Strategy.CondRddGrpMap import cond_rdd_grpmap
from .Strategy.CondRddMapPart import cond_rdd_mappart
from .Strategy.CondRddReduce import cond_rdd_reduce
from .Strategy.CondSqlJoin import cond_sql_join
from .Strategy.CondSqlNested import cond_sql_nested
from .Strategy.CondSqlNull import cond_sql_null


@dataclass(frozen=True)
class PythonTestMethod:
    strategy_name: str
    language: str
    interface: str
    delegate: Callable[
        [TidySparkSession, List[DataPoint]],
        Tuple[RDD | None, spark_DataFrame | None]]

# CondMethod = collections.namedtuple("CondMethod",
#                                     ["name", "interface", "delegate"])


implementation_list: List[PythonTestMethod] = [
    PythonTestMethod(
        strategy_name='cond_sql_join',
        language='python',
        interface='sql',
        delegate=cond_sql_join
    ),
    PythonTestMethod(
        strategy_name='cond_fluent_join',
        language='python',
        interface='fluent',
        delegate=cond_fluent_join
    ),
    PythonTestMethod(
        strategy_name='cond_sql_null',
        language='python',
        interface='sql',
        delegate=cond_sql_null
    ),
    PythonTestMethod(
        strategy_name='cond_fluent_null',
        language='python',
        interface='fluent',
        delegate=cond_fluent_null
    ),
    PythonTestMethod(
        strategy_name='cond_fluent_zero',
        language='python',
        interface='fluent',
        delegate=cond_fluent_zero
    ),
    PythonTestMethod(
        strategy_name='cond_pandas',
        language='python',
        interface='pandas',
        delegate=cond_pandas
    ),
    PythonTestMethod(
        strategy_name='cond_pandas_numba',
        language='python',
        interface='pandas',
        delegate=cond_pandas_numba
    ),
    PythonTestMethod(
        strategy_name='cond_sql_nested',
        language='python',
        interface='sql',
        delegate=cond_sql_nested
    ),
    PythonTestMethod(
        strategy_name='cond_fluent_nested',
        language='python',
        interface='fluent',
        delegate=cond_fluent_nested
    ),
    PythonTestMethod(
        strategy_name='cond_fluent_window',
        language='python',
        interface='fluent',
        delegate=cond_fluent_window
    ),
    PythonTestMethod(
        strategy_name='cond_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=cond_rdd_grpmap
    ),
    PythonTestMethod(
        strategy_name='cond_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=cond_rdd_reduce
    ),
    PythonTestMethod(
        strategy_name='cond_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=cond_rdd_mappart
    ),
]


strategy_name_list = [x.strategy_name for x in implementation_list]
