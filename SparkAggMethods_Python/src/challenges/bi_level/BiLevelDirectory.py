from typing import List

from challenges.bi_level.strategies.using_pyspark.BiLevelFluentJoin import \
    bi_fluent_join
from challenges.bi_level.strategies.using_pyspark.BiLevelFluentNested import \
    bi_fluent_nested
from challenges.bi_level.strategies.using_pyspark.BiLevelFluentWindow import \
    bi_fluent_window
from challenges.bi_level.strategies.using_pyspark.BiLevelPandas import \
    bi_pandas
from challenges.bi_level.strategies.using_pyspark.BiLevelPandasNumba import \
    bi_pandas_numba
from challenges.bi_level.strategies.using_pyspark.BiLevelRddGrpMap import \
    bi_rdd_grpmap
from challenges.bi_level.strategies.using_pyspark.BiLevelRddMapPart import \
    bi_rdd_mappart
from challenges.bi_level.strategies.using_pyspark.BiLevelRddReduce1 import \
    bi_rdd_reduce1
from challenges.bi_level.strategies.using_pyspark.BiLevelRddReduce2 import \
    bi_rdd_reduce2
from challenges.bi_level.strategies.using_pyspark.BiLevelSqlJoin import \
    bi_sql_join
from challenges.bi_level.strategies.using_pyspark.BiLevelSqlNested import \
    bi_sql_nested
from six_field_test_data.six_generate_test_data_using_pyspark import \
    PysparkPythonTestMethod

pyspark_implementation_list: List[PysparkPythonTestMethod] = [
    PysparkPythonTestMethod(
        original_strategy_name='bi_sql_join',
        strategy_name='bi_sql_join',
        language='python',
        interface='sql',
        delegate=bi_sql_join
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_fluent_join',
        strategy_name='bi_fluent_join',
        language='python',
        interface='fluent',
        delegate=bi_fluent_join
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_pandas',
        strategy_name='bi_pandas',
        language='python',
        interface='pandas',
        delegate=bi_pandas
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_pandas_numba',
        strategy_name='bi_pandas_numba',
        language='python',
        interface='pandas',
        delegate=bi_pandas_numba
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_sql_nested',
        strategy_name='bi_sql_nested',
        language='python',
        interface='sql',
        delegate=bi_sql_nested
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_fluent_nested',
        strategy_name='bi_fluent_nested',
        language='python',
        interface='fluent',
        delegate=bi_fluent_nested
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_fluent_window',
        strategy_name='bi_fluent_window',
        language='python',
        interface='fluent',
        delegate=bi_fluent_window
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_rdd_grpmap',
        strategy_name='bi_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=bi_rdd_grpmap
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_rdd_reduce1',
        strategy_name='bi_rdd_reduce1',
        language='python',
        interface='rdd',
        delegate=bi_rdd_reduce1
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_rdd_reduce2',
        strategy_name='bi_rdd_reduce2',
        language='python',
        interface='rdd',
        delegate=bi_rdd_reduce2
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_rdd_mappart',
        strategy_name='bi_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=bi_rdd_mappart
    ),
]


strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]
