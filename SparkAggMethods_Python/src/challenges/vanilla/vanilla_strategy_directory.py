from src.challenges.vanilla.using_dask.vanilla_dask_bag_accumulate import \
    vanilla_dask_bag_accumulate
from src.challenges.vanilla.using_dask.vanilla_dask_bag_fold import \
    vanilla_dask_bag_fold
from src.challenges.vanilla.using_dask.vanilla_dask_bag_foldby import \
    vanilla_dask_bag_foldby
from src.challenges.vanilla.using_dask.vanilla_dask_bag_map_partitions import \
    vanilla_dask_bag_map_partitions
from src.challenges.vanilla.using_dask.vanilla_dask_bag_reduction import \
    vanilla_dask_bag_reduction
from src.challenges.vanilla.using_dask.vanilla_dask_ddf_grp_apply import \
    vanilla_dask_ddf_grp_apply
from src.challenges.vanilla.using_dask.vanilla_dask_ddf_grp_udaf import \
    vanilla_dask_ddf_grp_udaf
from src.challenges.vanilla.using_dask.vanilla_dask_sql import \
    vanilla_dask_sql_no_gpu
from src.challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_builtin import \
    vanilla_pyspark_df_grp_builtin
from src.challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_numba import \
    vanilla_pyspark_df_grp_numba
from src.challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_numpy import \
    vanilla_pyspark_df_grp_numpy
from src.challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_pandas import \
    vanilla_pyspark_df_grp_pandas
from src.challenges.vanilla.using_pyspark.vanilla_pyspark_rdd_grp_map import \
    vanilla_pyspark_rdd_grp_map
from src.challenges.vanilla.using_pyspark.vanilla_pyspark_rdd_mappart import \
    vanilla_pyspark_rdd_mappart
from src.challenges.vanilla.using_pyspark.vanilla_pyspark_rdd_reduce import \
    vanilla_pyspark_rdd_reduce
from src.challenges.vanilla.using_pyspark.vanilla_pyspark_sql import \
    vanilla_pyspark_sql
from src.challenges.vanilla.using_python_only.vanilla_py_only_pd_grp_numba import \
    vanilla_py_only_pd_grp_numba
from src.challenges.vanilla.using_python_only.vanilla_py_only_pd_grp_numpy import \
    vanilla_py_only_pd_grp_numpy
from src.perf_test_common import ChallengeMethodRegistration
from src.six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonDaskRegistration,
    ChallengeMethodPythonOnlyRegistration,
    ChallengeMethodPythonPysparkRegistration)
from src.six_field_test_data.six_generate_test_data.six_test_data_for_python_only import \
    NumericalToleranceExpectations
from src.utils.inspection import name_of_function

STRATEGIES_USING_DASK_REGISTRY: list[ChallengeMethodPythonDaskRegistration] = [
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_dask_bag_accumulate),
        language='python',
        interface='bag',
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        delegate=vanilla_dask_bag_accumulate,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_dask_bag_fold),
        language='python',
        interface='bag',
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        delegate=vanilla_dask_bag_fold,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_dask_bag_foldby),
        language='python',
        interface='bag',
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        delegate=vanilla_dask_bag_foldby,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_dask_bag_map_partitions),
        language='python',
        interface='bag',
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        delegate=vanilla_dask_bag_map_partitions,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_dask_bag_reduction),
        language='python',
        interface='bag',
        numerical_tolerance=NumericalToleranceExpectations.SIMPLE_SUM,
        requires_gpu=False,
        delegate=vanilla_dask_bag_reduction,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_dask_ddf_grp_apply),
        language='python',
        interface='ddf',
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_dask_ddf_grp_apply,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_dask_ddf_grp_udaf),
        language='python',
        interface='ddf',
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_dask_ddf_grp_udaf,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_dask_sql_no_gpu),
        language='python',
        interface='ddf',
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_dask_sql_no_gpu,
    ),
]

STRATEGIES_USING_PYSPARK_REGISTRY: list[ChallengeMethodPythonPysparkRegistration] = [
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_sql',
        strategy_name=name_of_function(vanilla_pyspark_sql),
        language='python',
        interface='sql',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=vanilla_pyspark_sql
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_fluent',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_builtin),
        language='python',
        interface='sql',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=vanilla_pyspark_df_grp_builtin,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_pandas',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_pandas),
        language='python',
        interface='pandas',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=vanilla_pyspark_df_grp_pandas,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_pandas_numpy',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_numpy),
        language='python',
        interface='pandas',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=vanilla_pyspark_df_grp_numpy,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_pandas_numba',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_numba),
        language='python',
        interface='pandas',
        requires_gpu=True,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=vanilla_pyspark_df_grp_numba,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_rdd_grpmap',
        strategy_name=name_of_function(vanilla_pyspark_rdd_grp_map),
        language='python',
        interface='rdd',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=vanilla_pyspark_rdd_grp_map,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_rdd_reduce',
        strategy_name=name_of_function(vanilla_pyspark_rdd_reduce),
        language='python',
        interface='rdd',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=vanilla_pyspark_rdd_reduce,
    ),
    ChallengeMethodPythonPysparkRegistration(
        strategy_name_2018='vanilla_rdd_mappart',
        strategy_name=name_of_function(vanilla_pyspark_rdd_mappart),
        language='python',
        interface='rdd',
        requires_gpu=False,
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        delegate=vanilla_pyspark_rdd_mappart,
    ),
]
STRATEGIES_USING_PYTHON_ONLY_REGISTRY: list[ChallengeMethodPythonOnlyRegistration] = [
    ChallengeMethodPythonOnlyRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_py_only_pd_grp_numba),
        language='python',
        interface='pandas',
        numerical_tolerance=NumericalToleranceExpectations.NUMBA,
        requires_gpu=True,
        delegate=vanilla_py_only_pd_grp_numba,
    ),
    ChallengeMethodPythonOnlyRegistration(
        strategy_name_2018=None,
        strategy_name=name_of_function(vanilla_py_only_pd_grp_numpy),
        language='python',
        interface='pandas',
        numerical_tolerance=NumericalToleranceExpectations.NUMPY,
        requires_gpu=False,
        delegate=vanilla_py_only_pd_grp_numpy,
    ),
]
STRATEGIES_USING_SCALA_REGISTRY = [
    ChallengeMethodRegistration(
        strategy_name_2018='vanilla_pyspark_sql',
        strategy_name='vanilla_sc_spark_sql',
        language='scala',
        interface='sql',
        requires_gpu=False,
    ),
    ChallengeMethodRegistration(
        strategy_name_2018='vanilla_fluent',
        strategy_name='vanilla_sc_spark_fluent',
        language='scala',
        interface='sql',
        requires_gpu=False,
    ),
    ChallengeMethodRegistration(
        strategy_name_2018='vanilla_udaf',
        strategy_name='vanilla_sc_spark_udaf',
        language='scala',
        interface='sql',
        requires_gpu=False,
    ),
    ChallengeMethodRegistration(
        strategy_name_2018='vanilla_rdd_grpmap',
        strategy_name='vanilla_sc_spark_rdd_grpmap',
        language='scala',
        interface='rdd',
        requires_gpu=False,
    ),
    ChallengeMethodRegistration(
        strategy_name_2018='vanilla_rdd_reduce',
        strategy_name='vanilla_sc_spark_rdd_reduce',
        language='scala',
        interface='rdd',
        requires_gpu=False,
    ),
    ChallengeMethodRegistration(
        strategy_name_2018='vanilla_rdd_mappart',
        strategy_name='vanilla_sc_spark_rdd_mappart',
        language='scala',
        interface='rdd',
        requires_gpu=False,
    ),
]
