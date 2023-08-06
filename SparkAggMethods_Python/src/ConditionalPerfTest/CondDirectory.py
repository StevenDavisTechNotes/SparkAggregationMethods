from typing import List

from SixFieldCommon.SixFieldTestData import PythonTestMethod

from ConditionalPerfTest.Strategy.CondFluentJoin import cond_fluent_join
from ConditionalPerfTest.Strategy.CondFluentNested import cond_fluent_nested
from ConditionalPerfTest.Strategy.CondFluentNull import cond_fluent_null
from ConditionalPerfTest.Strategy.CondFluentWindow import cond_fluent_window
from ConditionalPerfTest.Strategy.CondFluentZero import cond_fluent_zero
from ConditionalPerfTest.Strategy.CondPandas import cond_pandas
from ConditionalPerfTest.Strategy.CondPandasNumba import cond_pandas_numba
from ConditionalPerfTest.Strategy.CondRddGrpMap import cond_rdd_grpmap
from ConditionalPerfTest.Strategy.CondRddMapPart import cond_rdd_mappart
from ConditionalPerfTest.Strategy.CondRddReduce import cond_rdd_reduce
from ConditionalPerfTest.Strategy.CondSqlJoin import cond_sql_join
from ConditionalPerfTest.Strategy.CondSqlNested import cond_sql_nested
from ConditionalPerfTest.Strategy.CondSqlNull import cond_sql_null

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
