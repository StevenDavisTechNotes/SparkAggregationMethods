from challenges.vanilla.using_dask.vanilla_dask_ddf_grp_apply import \
    vanilla_dask_ddf_grp_apply
from challenges.vanilla.using_dask.vanilla_dask_ddf_grp_udaf import \
    vanilla_dask_ddf_grp_udaf
from challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_builtin import \
    vanilla_pyspark_df_grp_builtin
from challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_numba import \
    vanilla_pyspark_df_grp_numba
from challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_numpy import \
    vanilla_pyspark_df_grp_numpy
from challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_pandas import \
    vanilla_pyspark_df_grp_pandas
from challenges.vanilla.using_pyspark.vanilla_pyspark_rdd_grp_map import \
    vanilla_pyspark_rdd_grp_map
from challenges.vanilla.using_pyspark.vanilla_pyspark_rdd_mappart import \
    vanilla_pyspark_rdd_mappart
from challenges.vanilla.using_pyspark.vanilla_pyspark_rdd_reduce import \
    vanilla_pyspark_rdd_reduce
from challenges.vanilla.using_pyspark.vanilla_pyspark_sql import \
    vanilla_pyspark_sql
from challenges.vanilla.using_python_only.vanilla_py_only_pd_grp_numba import \
    vanilla_py_only_pd_grp_numba
from challenges.vanilla.using_python_only.vanilla_py_only_pd_grp_numpy import \
    vanilla_py_only_pd_grp_numpy
from perf_test_common import ChallengeMethodExternalRegistration
from six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonDaskRegistration,
    ChallengeMethodPythonOnlyRegistration,
    ChallengeMethodPythonPysparkRegistration)
from utils.inspection import name_of_function

SOLUTIONS_USING_DASK_REGISTRY: list[ChallengeMethodPythonDaskRegistration] = [
    ChallengeMethodPythonDaskRegistration(
        strategy_name=name_of_function(vanilla_dask_ddf_grp_apply),
        language='python',
        interface='ddf',
        requires_gpu=False,
        delegate=vanilla_dask_ddf_grp_apply,
    ),
    ChallengeMethodPythonDaskRegistration(
        strategy_name=name_of_function(vanilla_dask_ddf_grp_udaf),
        language='python',
        interface='ddf',
        requires_gpu=False,
        delegate=vanilla_dask_ddf_grp_udaf,
    ),
]

SOLUTIONS_USING_PYSPARK_REGISTRY: list[ChallengeMethodPythonPysparkRegistration] = [
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_sql',
        strategy_name=name_of_function(vanilla_pyspark_sql),
        language='python',
        interface='sql',
        requires_gpu=False,
        delegate=vanilla_pyspark_sql
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_fluent',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_builtin),
        language='python',
        interface='sql',
        requires_gpu=False,
        delegate=vanilla_pyspark_df_grp_builtin,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_pandas',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_pandas),
        language='python',
        interface='pandas',
        requires_gpu=False,
        delegate=vanilla_pyspark_df_grp_pandas,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_pandas_numpy',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_numpy),
        language='python',
        interface='pandas',
        requires_gpu=False,
        delegate=vanilla_pyspark_df_grp_numpy,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_pandas_numba',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_numba),
        language='python',
        interface='pandas',
        requires_gpu=True,
        delegate=vanilla_pyspark_df_grp_numba,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_rdd_grpmap',
        strategy_name=name_of_function(vanilla_pyspark_rdd_grp_map),
        language='python',
        interface='rdd',
        requires_gpu=False,
        delegate=vanilla_pyspark_rdd_grp_map,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_rdd_reduce',
        strategy_name=name_of_function(vanilla_pyspark_rdd_reduce),
        language='python',
        interface='rdd',
        requires_gpu=False,
        delegate=vanilla_pyspark_rdd_reduce,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_rdd_mappart',
        strategy_name=name_of_function(vanilla_pyspark_rdd_mappart),
        language='python',
        interface='rdd',
        requires_gpu=False,
        delegate=vanilla_pyspark_rdd_mappart,
    ),
]
SOLUTIONS_USING_PYTHON_ST_REGISTRY: list[ChallengeMethodPythonOnlyRegistration] = [
    ChallengeMethodPythonOnlyRegistration(
        strategy_name=name_of_function(vanilla_py_only_pd_grp_numba),
        language='python',
        interface='pandas',
        delegate=vanilla_py_only_pd_grp_numba,
    ),
    ChallengeMethodPythonOnlyRegistration(
        strategy_name=name_of_function(vanilla_py_only_pd_grp_numpy),
        language='python',
        interface='pandas',
        delegate=vanilla_py_only_pd_grp_numpy,
    ),
]
SOLUTIONS_USING_SCALA_REGISTRY = [
    ChallengeMethodExternalRegistration(
        strategy_name='vanilla_pyspark_sql',
        language='scala',
        interface='sql'),
    ChallengeMethodExternalRegistration(
        strategy_name='vanilla_fluent',
        language='scala',
        interface='sql'),
    ChallengeMethodExternalRegistration(
        strategy_name='vanilla_udaf',
        language='scala',
        interface='sql'),
    ChallengeMethodExternalRegistration(
        strategy_name='vanilla_rdd_grpmap',
        language='scala',
        interface='rdd'),
    ChallengeMethodExternalRegistration(
        strategy_name='vanilla_rdd_reduce',
        language='scala',
        interface='rdd'),
    ChallengeMethodExternalRegistration(
        strategy_name='vanilla_rdd_mappart',
        language='scala',
        interface='rdd'),
]

STRATEGY_NAME_LIST_DASK = [x.strategy_name for x in SOLUTIONS_USING_DASK_REGISTRY]
STRATEGY_NAME_LIST_PYSPARK = [x.strategy_name for x in SOLUTIONS_USING_PYSPARK_REGISTRY]
STRATEGY_NAME_LIST_PYTHON_ONLY = [x.strategy_name for x in SOLUTIONS_USING_PYTHON_ST_REGISTRY]
