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
from six_field_test_data.six_generate_test_data import \
    ChallengeMethodPythonPysparkRegistration
from utils.inspection import name_of_function

# solutions_using_dask: list[ChallengeMethodPythonDaskRegistration] = [
# ]
# solutions_using_python_only: list[ChallengeMethodPythonOnlyRegistration] = [
# ]

solutions_using_pyspark: list[ChallengeMethodPythonPysparkRegistration] = [
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_sql_join',
        strategy_name=name_of_function(cond_pyspark_sql_join),
        language='python',
        interface='sql',
        requires_gpu=False,
        delegate=cond_pyspark_sql_join
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_fluent_join',
        strategy_name=name_of_function(cond_pyspark_df_join),
        language='python',
        interface='fluent',
        requires_gpu=False,
        delegate=cond_pyspark_df_join
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_sql_null',
        strategy_name=name_of_function(cond_pyspark_sql_null),
        language='python',
        interface='sql',
        requires_gpu=False,
        delegate=cond_pyspark_sql_null
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_fluent_null',
        strategy_name=name_of_function(cond_pyspark_df_null),
        language='python',
        interface='fluent',
        requires_gpu=False,
        delegate=cond_pyspark_df_null
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_fluent_zero',
        strategy_name=name_of_function(cond_pyspark_df_zero),
        language='python',
        interface='fluent',
        requires_gpu=False,
        delegate=cond_pyspark_df_zero
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_pandas',
        strategy_name=name_of_function(cond_pyspark_df_grp_pandas),
        language='python',
        interface='pandas',
        requires_gpu=False,
        delegate=cond_pyspark_df_grp_pandas
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_pandas_numba',
        strategy_name=name_of_function(cond_pyspark_df_grp_pandas_numba),
        language='python',
        interface='pandas',
        requires_gpu=False,
        delegate=cond_pyspark_df_grp_pandas_numba
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_sql_nested',
        strategy_name=name_of_function(cond_pyspark_sql_nested),
        language='python',
        interface='sql',
        requires_gpu=False,
        delegate=cond_pyspark_sql_nested
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_fluent_nested',
        strategy_name=name_of_function(cond_pyspark_df_nested),
        language='python',
        interface='fluent',
        requires_gpu=False,
        delegate=cond_pyspark_df_nested
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_fluent_window',
        strategy_name=name_of_function(cond_pyspark_df_window),
        language='python',
        interface='fluent',
        requires_gpu=False,
        delegate=cond_pyspark_df_window
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_rdd_grpmap',
        strategy_name=name_of_function(cond_pyspark_rdd_grp_map),
        language='python',
        interface='rdd',
        requires_gpu=False,
        delegate=cond_pyspark_rdd_grp_map
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_rdd_reduce',
        strategy_name=name_of_function(cond_pyspark_rdd_reduce),
        language='python',
        interface='rdd',
        requires_gpu=False,
        delegate=cond_pyspark_rdd_reduce
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='cond_rdd_mappart',
        strategy_name=name_of_function(cond_pyspark_rdd_map_part),
        language='python',
        interface='rdd',
        requires_gpu=False,
        delegate=cond_pyspark_rdd_map_part
    ),
]


STRATEGY_NAME_LIST = [x.strategy_name for x in solutions_using_pyspark]
