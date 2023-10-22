from typing import List

from ConditionalPerfTest.PySparkStrategy.CondFluentJoin import cond_fluent_join
from ConditionalPerfTest.PySparkStrategy.CondFluentNested import \
    cond_fluent_nested
from ConditionalPerfTest.PySparkStrategy.CondFluentNull import cond_fluent_null
from ConditionalPerfTest.PySparkStrategy.CondFluentWindow import \
    cond_fluent_window
from ConditionalPerfTest.PySparkStrategy.CondFluentZero import cond_fluent_zero
from ConditionalPerfTest.PySparkStrategy.CondPandas import cond_pandas
from ConditionalPerfTest.PySparkStrategy.CondPandasNumba import \
    cond_pandas_numba
from ConditionalPerfTest.PySparkStrategy.CondRddGrpMap import cond_rdd_grpmap
from ConditionalPerfTest.PySparkStrategy.CondRddMapPart import cond_rdd_mappart
from ConditionalPerfTest.PySparkStrategy.CondRddReduce import cond_rdd_reduce
from ConditionalPerfTest.PySparkStrategy.CondSqlJoin import cond_sql_join
from ConditionalPerfTest.PySparkStrategy.CondSqlNested import cond_sql_nested
from ConditionalPerfTest.PySparkStrategy.CondSqlNull import cond_sql_null
from SixFieldCommon.PySpark_SixFieldTestData import PysparkPythonTestMethod

pyspark_implementation_list: List[PysparkPythonTestMethod] = [
    PysparkPythonTestMethod(
        strategy_name='cond_sql_join',
        language='python',
        interface='sql',
        delegate=cond_sql_join
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_fluent_join',
        language='python',
        interface='fluent',
        delegate=cond_fluent_join
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_sql_null',
        language='python',
        interface='sql',
        delegate=cond_sql_null
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_fluent_null',
        language='python',
        interface='fluent',
        delegate=cond_fluent_null
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_fluent_zero',
        language='python',
        interface='fluent',
        delegate=cond_fluent_zero
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_pandas',
        language='python',
        interface='pandas',
        delegate=cond_pandas
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_pandas_numba',
        language='python',
        interface='pandas',
        delegate=cond_pandas_numba
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_sql_nested',
        language='python',
        interface='sql',
        delegate=cond_sql_nested
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_fluent_nested',
        language='python',
        interface='fluent',
        delegate=cond_fluent_nested
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_fluent_window',
        language='python',
        interface='fluent',
        delegate=cond_fluent_window
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=cond_rdd_grpmap
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=cond_rdd_reduce
    ),
    PysparkPythonTestMethod(
        strategy_name='cond_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=cond_rdd_mappart
    ),
]


strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]
