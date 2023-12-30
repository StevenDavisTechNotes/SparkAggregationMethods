from typing import List

from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_grp_pandas import \
    bi_level_pyspark_df_grp_pandas
from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_grp_pandas_numba import \
    bi_level_pyspark_df_grp_pandas_numba
from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_join import \
    bi_level_pyspark_df_join
from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_nested import \
    bi_level_pyspark_df_nested
from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_window import \
    bi_level_pyspark_df_window
from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_grp_map import \
    bi_level_pyspark_rdd_grp_map
from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_map_part import \
    bi_level_pyspark_rdd_map_part
from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_reduce_1 import \
    bi_level_pyspark_rdd_reduce_1
from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_reduce_2 import \
    bi_level_pyspark_rdd_reduce_2
from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_sql_join import \
    bi_level_pyspark_sql_join
from challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_sql_nested import \
    bi_level_pyspark_sql_nested
from six_field_test_data.six_generate_test_data_using_pyspark import \
    PysparkPythonTestMethod
from utils.inspection import nameof

pyspark_implementation_list: List[PysparkPythonTestMethod] = [
    PysparkPythonTestMethod(
        original_strategy_name='bi_sql_join',
        strategy_name=nameof(bi_level_pyspark_sql_join),
        language='python',
        interface='sql',
        delegate=bi_level_pyspark_sql_join
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_fluent_join',
        strategy_name=nameof(bi_level_pyspark_df_join),
        language='python',
        interface='fluent',
        delegate=bi_level_pyspark_df_join
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_pandas',
        strategy_name=nameof(bi_level_pyspark_df_grp_pandas),
        language='python',
        interface='pandas',
        delegate=bi_level_pyspark_df_grp_pandas
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_pandas_numba',
        strategy_name=nameof(bi_level_pyspark_df_grp_pandas_numba),
        language='python',
        interface='pandas',
        delegate=bi_level_pyspark_df_grp_pandas_numba
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_sql_nested',
        strategy_name=nameof(bi_level_pyspark_sql_nested),
        language='python',
        interface='sql',
        delegate=bi_level_pyspark_sql_nested
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_fluent_nested',
        strategy_name=nameof(bi_level_pyspark_df_nested),
        language='python',
        interface='fluent',
        delegate=bi_level_pyspark_df_nested
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_fluent_window',
        strategy_name=nameof(bi_level_pyspark_df_window),
        language='python',
        interface='fluent',
        delegate=bi_level_pyspark_df_window
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_rdd_grpmap',
        strategy_name=nameof(bi_level_pyspark_rdd_grp_map),
        language='python',
        interface='rdd',
        delegate=bi_level_pyspark_rdd_grp_map
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_rdd_reduce1',
        strategy_name=nameof(bi_level_pyspark_rdd_reduce_1),
        language='python',
        interface='rdd',
        delegate=bi_level_pyspark_rdd_reduce_1
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_rdd_reduce2',
        strategy_name=nameof(bi_level_pyspark_rdd_reduce_2),
        language='python',
        interface='rdd',
        delegate=bi_level_pyspark_rdd_reduce_2
    ),
    PysparkPythonTestMethod(
        original_strategy_name='bi_rdd_mappart',
        strategy_name=nameof(bi_level_pyspark_rdd_map_part),
        language='python',
        interface='rdd',
        delegate=bi_level_pyspark_rdd_map_part
    ),
]


strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]