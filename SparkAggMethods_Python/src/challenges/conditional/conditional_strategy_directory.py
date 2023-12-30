from typing import List

from challenges.conditional.strategies.using_pyspark.cond_pyspark_df_grp_pandas import \
    cond_pyspark_df_grp_pandas
from challenges.conditional.strategies.using_pyspark.cond_pyspark_df_grp_pandas_numba import \
    cond_pyspark_df_grp_pandas_numba
from challenges.conditional.strategies.using_pyspark.cond_pyspark_df_join import \
    cond_pyspark_df_join
from challenges.conditional.strategies.using_pyspark.cond_pyspark_df_nested import \
    cond_pyspark_df_nested
from challenges.conditional.strategies.using_pyspark.cond_pyspark_df_null import \
    cond_pyspark_df_null
from challenges.conditional.strategies.using_pyspark.cond_pyspark_df_window import \
    cond_pyspark_df_window
from challenges.conditional.strategies.using_pyspark.cond_pyspark_df_zero import \
    cond_pyspark_df_zero
from challenges.conditional.strategies.using_pyspark.cond_pyspark_rdd_grp_map import \
    cond_pyspark_rdd_grp_map
from challenges.conditional.strategies.using_pyspark.cond_pyspark_rdd_map_part import \
    cond_pyspark_rdd_map_part
from challenges.conditional.strategies.using_pyspark.cond_pyspark_rdd_reduce import \
    cond_pyspark_rdd_reduce
from challenges.conditional.strategies.using_pyspark.cond_pyspark_sql_join import \
    cond_pyspark_sql_join
from challenges.conditional.strategies.using_pyspark.cond_pyspark_sql_nested import \
    cond_pyspark_sql_nested
from challenges.conditional.strategies.using_pyspark.cond_pyspark_sql_null import \
    cond_pyspark_sql_null
from six_field_test_data.six_generate_test_data_using_pyspark import \
    PysparkPythonTestMethod
from utils.inspection import nameof

pyspark_implementation_list: List[PysparkPythonTestMethod] = [
    PysparkPythonTestMethod(
        original_strategy_name='cond_sql_join',
        strategy_name=nameof(cond_pyspark_sql_join),
        language='python',
        interface='sql',
        delegate=cond_pyspark_sql_join
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_fluent_join',
        strategy_name=nameof(cond_pyspark_df_join),
        language='python',
        interface='fluent',
        delegate=cond_pyspark_df_join
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_sql_null',
        strategy_name=nameof(cond_pyspark_sql_null),
        language='python',
        interface='sql',
        delegate=cond_pyspark_sql_null
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_fluent_null',
        strategy_name=nameof(cond_pyspark_df_null),
        language='python',
        interface='fluent',
        delegate=cond_pyspark_df_null
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_fluent_zero',
        strategy_name=nameof(cond_pyspark_df_zero),
        language='python',
        interface='fluent',
        delegate=cond_pyspark_df_zero
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_pandas',
        strategy_name=nameof(cond_pyspark_df_grp_pandas),
        language='python',
        interface='pandas',
        delegate=cond_pyspark_df_grp_pandas
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_pandas_numba',
        strategy_name=nameof(cond_pyspark_df_grp_pandas_numba),
        language='python',
        interface='pandas',
        delegate=cond_pyspark_df_grp_pandas_numba
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_sql_nested',
        strategy_name=nameof(cond_pyspark_sql_nested),
        language='python',
        interface='sql',
        delegate=cond_pyspark_sql_nested
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_fluent_nested',
        strategy_name=nameof(cond_pyspark_df_nested),
        language='python',
        interface='fluent',
        delegate=cond_pyspark_df_nested
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_fluent_window',
        strategy_name=nameof(cond_pyspark_df_window),
        language='python',
        interface='fluent',
        delegate=cond_pyspark_df_window
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_rdd_grpmap',
        strategy_name=nameof(cond_pyspark_rdd_grp_map),
        language='python',
        interface='rdd',
        delegate=cond_pyspark_rdd_grp_map
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_rdd_reduce',
        strategy_name=nameof(cond_pyspark_rdd_reduce),
        language='python',
        interface='rdd',
        delegate=cond_pyspark_rdd_reduce
    ),
    PysparkPythonTestMethod(
        original_strategy_name='cond_rdd_mappart',
        strategy_name=nameof(cond_pyspark_rdd_map_part),
        language='python',
        interface='rdd',
        delegate=cond_pyspark_rdd_map_part
    ),
]


strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]
