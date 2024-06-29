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
    ChallengeMethodPythonPysparkRegistration
from t_utils.inspection import name_of_function

pyspark_implementation_list: list[ChallengeMethodPythonPysparkRegistration] = [
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_sql_join',
        strategy_name=name_of_function(bi_level_pyspark_sql_join),
        language='python',
        interface='sql',
        only_when_gpu_testing=False,
        delegate=bi_level_pyspark_sql_join
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_fluent_join',
        strategy_name=name_of_function(bi_level_pyspark_df_join),
        language='python',
        interface='fluent',
        only_when_gpu_testing=False,
        delegate=bi_level_pyspark_df_join
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_pandas',
        strategy_name=name_of_function(bi_level_pyspark_df_grp_pandas),
        language='python',
        interface='pandas',
        only_when_gpu_testing=False,
        delegate=bi_level_pyspark_df_grp_pandas
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_pandas_numba',
        strategy_name=name_of_function(bi_level_pyspark_df_grp_pandas_numba),
        language='python',
        interface='pandas',
        only_when_gpu_testing=True,
        delegate=bi_level_pyspark_df_grp_pandas_numba
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_sql_nested',
        strategy_name=name_of_function(bi_level_pyspark_sql_nested),
        language='python',
        interface='sql',
        only_when_gpu_testing=False,
        delegate=bi_level_pyspark_sql_nested
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_fluent_nested',
        strategy_name=name_of_function(bi_level_pyspark_df_nested),
        language='python',
        interface='fluent',
        only_when_gpu_testing=False,
        delegate=bi_level_pyspark_df_nested
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_fluent_window',
        strategy_name=name_of_function(bi_level_pyspark_df_window),
        language='python',
        interface='fluent',
        only_when_gpu_testing=False,
        delegate=bi_level_pyspark_df_window
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_rdd_grpmap',
        strategy_name=name_of_function(bi_level_pyspark_rdd_grp_map),
        language='python',
        interface='rdd',
        only_when_gpu_testing=False,
        delegate=bi_level_pyspark_rdd_grp_map
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_rdd_reduce1',
        strategy_name=name_of_function(bi_level_pyspark_rdd_reduce_1),
        language='python',
        interface='rdd',
        only_when_gpu_testing=False,
        delegate=bi_level_pyspark_rdd_reduce_1
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_rdd_reduce2',
        strategy_name=name_of_function(bi_level_pyspark_rdd_reduce_2),
        language='python',
        interface='rdd',
        only_when_gpu_testing=False,
        delegate=bi_level_pyspark_rdd_reduce_2
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='bi_rdd_mappart',
        strategy_name=name_of_function(bi_level_pyspark_rdd_map_part),
        language='python',
        interface='rdd',
        only_when_gpu_testing=False,
        delegate=bi_level_pyspark_rdd_map_part
    ),
]


STRATEGY_NAME_LIST = [x.strategy_name for x in pyspark_implementation_list]
