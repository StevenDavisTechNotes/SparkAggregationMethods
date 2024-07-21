from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_grp_pandas import \
    bi_level_pyspark_df_grp_pandas
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_grp_pandas_numba import \
    bi_level_pyspark_df_grp_pandas_numba
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_join import \
    bi_level_pyspark_df_join
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_nested import \
    bi_level_pyspark_df_nested
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_df_window import \
    bi_level_pyspark_df_window
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_grp_map import \
    bi_level_pyspark_rdd_grp_map
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_map_part import \
    bi_level_pyspark_rdd_map_part
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_reduce_1 import \
    bi_level_pyspark_rdd_reduce_1
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_rdd_reduce_2 import \
    bi_level_pyspark_rdd_reduce_2
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_sql_join import \
    bi_level_pyspark_sql_join
from src.challenges.bi_level.strategies.using_pyspark.bi_level_pyspark_sql_nested import \
    bi_level_pyspark_sql_nested
from src.six_field_test_data.six_generate_test_data import \
    ChallengeMethodPythonPysparkRegistration
from src.six_field_test_data.six_generate_test_data.six_test_data_for_python_only import \
    NumericalToleranceExpectations
from src.utils.inspection import name_of_function

# solutions_using_dask: list[ChallengeMethodPythonDaskRegistration] = [
# ]
# solutions_using_python_only: list[ChallengeMethodPythonOnlyRegistration] = [
# ]
solutions_using_pyspark: list[ChallengeMethodPythonPysparkRegistration] = [
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_sql_join',
        strategy_name=name_of_function(bi_level_pyspark_sql_join),
        language='python',
        interface='sql',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_sql_join
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_fluent_join',
        strategy_name=name_of_function(bi_level_pyspark_df_join),
        language='python',
        interface='fluent',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_df_join
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_pandas',
        strategy_name=name_of_function(bi_level_pyspark_df_grp_pandas),
        language='python',
        interface='pandas',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_df_grp_pandas
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_pandas_numba',
        strategy_name=name_of_function(bi_level_pyspark_df_grp_pandas_numba),
        language='python',
        interface='pandas',
        requires_gpu=True,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_df_grp_pandas_numba
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_sql_nested',
        strategy_name=name_of_function(bi_level_pyspark_sql_nested),
        language='python',
        interface='sql',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_sql_nested
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_fluent_nested',
        strategy_name=name_of_function(bi_level_pyspark_df_nested),
        language='python',
        interface='fluent',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_df_nested
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_fluent_window',
        strategy_name=name_of_function(bi_level_pyspark_df_window),
        language='python',
        interface='fluent',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_df_window
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_rdd_grpmap',
        strategy_name=name_of_function(bi_level_pyspark_rdd_grp_map),
        language='python',
        interface='rdd',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_rdd_grp_map
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_rdd_reduce1',
        strategy_name=name_of_function(bi_level_pyspark_rdd_reduce_1),
        language='python',
        interface='rdd',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_rdd_reduce_1
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_rdd_reduce2',
        strategy_name=name_of_function(bi_level_pyspark_rdd_reduce_2),
        language='python',
        interface='rdd',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_rdd_reduce_2
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='bi_rdd_mappart',
        strategy_name=name_of_function(bi_level_pyspark_rdd_map_part),
        language='python',
        interface='rdd',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=bi_level_pyspark_rdd_map_part
    ),
]


STRATEGY_NAME_LIST = [x.strategy_name for x in solutions_using_pyspark]
