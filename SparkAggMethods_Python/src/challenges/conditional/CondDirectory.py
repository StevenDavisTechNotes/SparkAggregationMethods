from typing import List

from challenges.conditional.strategies.using_pyspark.CondFluentJoin import \
    cond_fluent_join
from challenges.conditional.strategies.using_pyspark.CondFluentNested import \
    cond_fluent_nested
from challenges.conditional.strategies.using_pyspark.CondFluentNull import \
    cond_fluent_null
from challenges.conditional.strategies.using_pyspark.CondFluentWindow import \
    cond_fluent_window
from challenges.conditional.strategies.using_pyspark.CondFluentZero import \
    cond_fluent_zero
from challenges.conditional.strategies.using_pyspark.CondPandas import \
    cond_pandas
from challenges.conditional.strategies.using_pyspark.CondPandasNumba import \
    cond_pandas_numba
from challenges.conditional.strategies.using_pyspark.CondRddGrpMap import \
    cond_rdd_grpmap
from challenges.conditional.strategies.using_pyspark.CondRddMapPart import \
    cond_rdd_mappart
from challenges.conditional.strategies.using_pyspark.CondRddReduce import \
    cond_rdd_reduce
from challenges.conditional.strategies.using_pyspark.CondSqlJoin import \
    cond_sql_join
from challenges.conditional.strategies.using_pyspark.CondSqlNested import \
    cond_sql_nested
from challenges.conditional.strategies.using_pyspark.CondSqlNull import \
    cond_sql_null
from six_field_test_data.six_generate_test_data_using_pyspark import \
    PysparkPythonTestMethod

pyspark_implementation_list: List[PysparkPythonTestMethod] = [
    PysparkPythonTestMethod(
        original_strategy_name='cond_sql_join',
        strategy_name='cond_sql_join',
        language='python',
        interface='sql',
        delegate=cond_sql_join
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_fluent_join',
        strategy_name='cond_fluent_join',
        language='python',
        interface='fluent',
        delegate=cond_fluent_join
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_sql_null',
        strategy_name='cond_sql_null',
        language='python',
        interface='sql',
        delegate=cond_sql_null
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_fluent_null',
        strategy_name='cond_fluent_null',
        language='python',
        interface='fluent',
        delegate=cond_fluent_null
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_fluent_zero',
        strategy_name='cond_fluent_zero',
        language='python',
        interface='fluent',
        delegate=cond_fluent_zero
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_pandas',
        strategy_name='cond_pandas',
        language='python',
        interface='pandas',
        delegate=cond_pandas
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_pandas_numba',
        strategy_name='cond_pandas_numba',
        language='python',
        interface='pandas',
        delegate=cond_pandas_numba
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_sql_nested',
        strategy_name='cond_sql_nested',
        language='python',
        interface='sql',
        delegate=cond_sql_nested
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_fluent_nested',
        strategy_name='cond_fluent_nested',
        language='python',
        interface='fluent',
        delegate=cond_fluent_nested
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_fluent_window',
        strategy_name='cond_fluent_window',
        language='python',
        interface='fluent',
        delegate=cond_fluent_window
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_rdd_grpmap',
        strategy_name='cond_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=cond_rdd_grpmap
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_rdd_reduce',
        strategy_name='cond_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=cond_rdd_reduce
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_rdd_mappart',
        strategy_name='cond_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=cond_rdd_mappart
    ),
]


strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]
