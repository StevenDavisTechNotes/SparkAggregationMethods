from src.challenges.bi_level.strategies.using_dask.bi_level_dask_sql_nested import \
    bi_level_dask_sql_nested_no_gpu
from src.challenges.bi_level.strategies.using_dask.bi_level_dask_sql_single_join import \
    bi_level_dask_sql_single_join_no_gpu
from src.challenges.bi_level.strategies.using_dask.bi_level_dask_sql_temp_join import \
    bi_level_dask_sql_temp_join_no_gpu
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
from src.challenges.bi_level.strategies.using_python_only.bi_level_py_only_pd_grp_numpy import \
    bi_level_py_only_pd_grp_numpy
from src.six_field_test_data.six_generate_test_data import \
    ChallengeMethodPythonPysparkRegistration
from src.six_field_test_data.six_generate_test_data.six_test_data_for_dask import \
    ChallengeMethodPythonDaskRegistration
from src.six_field_test_data.six_generate_test_data.six_test_data_for_python_only import (
    ChallengeMethodPythonOnlyRegistration, NumericalToleranceExpectations)
from src.utils.inspection import name_of_function

STRATEGIES_USING_DASK_REGISTRY: list[ChallengeMethodPythonDaskRegistration] = [
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(bi_level_dask_sql_nested_no_gpu),
        language='python',
        interface='ddf',
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bi_level_dask_sql_nested_no_gpu,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(bi_level_dask_sql_single_join_no_gpu),
        language='python',
        interface='ddf',
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bi_level_dask_sql_single_join_no_gpu,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(bi_level_dask_sql_temp_join_no_gpu),
        language='python',
        interface='ddf',
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bi_level_dask_sql_temp_join_no_gpu,
    ),
]
STRATEGIES_USING_PYSPARK_REGISTRY: list[ChallengeMethodPythonPysparkRegistration] = [
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

STRATEGIES_USING_PYTHON_ONLY_REGISTRY: list[ChallengeMethodPythonOnlyRegistration] = [
    ChallengeMethodPythonOnlyRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(bi_level_py_only_pd_grp_numpy),
        language='python',
        interface='pandas',
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=bi_level_py_only_pd_grp_numpy,
    ),
]
