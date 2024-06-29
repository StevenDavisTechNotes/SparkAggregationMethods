from challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_builtin import \
    vanilla_pyspark_df_grp_builtin
from challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_pandas import \
    vanilla_pyspark_df_grp_pandas
from challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_pandas_numba import \
    vanilla_pyspark_df_grp_pandas_numba
from challenges.vanilla.using_pyspark.vanilla_pyspark_df_grp_pandas_numpy import \
    vanilla_pyspark_df_grp_pandas_numpy
from challenges.vanilla.using_pyspark.vanilla_pyspark_rdd_grp_map import \
    vanilla_pyspark_rdd_grp_map
from challenges.vanilla.using_pyspark.vanilla_pyspark_rdd_mappart import \
    vanilla_pyspark_rdd_mappart
from challenges.vanilla.using_pyspark.vanilla_pyspark_rdd_reduce import \
    vanilla_pyspark_rdd_reduce
from challenges.vanilla.using_pyspark.vanilla_pyspark_sql import \
    vanilla_pyspark_sql
from perf_test_common import ChallengeMethodExternalRegistration
from six_field_test_data.six_generate_test_data_using_dask import \
    ChallengeMethodPythonDaskRegistration
from six_field_test_data.six_generate_test_data_using_pyspark import \
    ChallengeMethodPythonPysparkRegistration
from utils.inspection import name_of_function

# from challenges.vanilla.using_dask.vanilla_dask_ddf_grp_apply import \
#     vanilla_dask_ddf_grp_apply
# from challenges.vanilla.using_dask.vanilla_dask_ddf_grp_udaf import \
#     vanilla_dask_ddf_grp_udaf
# from challenges.vanilla.using_dask.vanilla_dask_sql import vanilla_dask_sql


dask_implementation_list: list[ChallengeMethodPythonDaskRegistration] = [
    # ChallengeMethodPythonDaskRegistration(
    #     strategy_name=name_of_function(vanilla_dask_sql),
    #     language='python',
    #     interface='sql',
    #     delegate=vanilla_dask_sql
    # ),
    # ChallengeMethodPythonDaskRegistration(
    #     strategy_name=name_of_function(vanilla_dask_ddf_grp_apply),
    #     language='python',
    #     interface='ddf',
    #     delegate=vanilla_dask_ddf_grp_apply,
    # ),
    # ChallengeMethodPythonDaskRegistration(
    #     strategy_name=name_of_function(vanilla_dask_ddf_grp_udaf),
    #     language='python',
    #     interface='ddf',
    #     delegate=vanilla_dask_ddf_grp_udaf,
    # ),
]

pyspark_implementation_list: list[ChallengeMethodPythonPysparkRegistration] = [
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_sql',
        strategy_name=name_of_function(vanilla_pyspark_sql),
        language='python',
        interface='sql',
        only_when_gpu_testing=False,
        delegate=vanilla_pyspark_sql
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_fluent',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_builtin),
        language='python',
        interface='sql',
        only_when_gpu_testing=False,
        delegate=vanilla_pyspark_df_grp_builtin,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_pandas',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_pandas),
        language='python',
        interface='pandas',
        only_when_gpu_testing=False,
        delegate=vanilla_pyspark_df_grp_pandas,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_pandas_numpy',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_pandas_numpy),
        language='python',
        interface='pandas',
        only_when_gpu_testing=False,
        delegate=vanilla_pyspark_df_grp_pandas_numpy,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_pandas_numba',
        strategy_name=name_of_function(vanilla_pyspark_df_grp_pandas_numba),
        language='python',
        interface='pandas',
        only_when_gpu_testing=True,
        delegate=vanilla_pyspark_df_grp_pandas_numba,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_rdd_grpmap',
        strategy_name=name_of_function(vanilla_pyspark_rdd_grp_map),
        language='python',
        interface='rdd',
        only_when_gpu_testing=False,
        delegate=vanilla_pyspark_rdd_grp_map,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_rdd_reduce',
        strategy_name=name_of_function(vanilla_pyspark_rdd_reduce),
        language='python',
        interface='rdd',
        only_when_gpu_testing=False,
        delegate=vanilla_pyspark_rdd_reduce,
    ),
    ChallengeMethodPythonPysparkRegistration(
        original_strategy_name='vanilla_rdd_mappart',
        strategy_name=name_of_function(vanilla_pyspark_rdd_mappart),
        language='python',
        interface='rdd',
        only_when_gpu_testing=False,
        delegate=vanilla_pyspark_rdd_mappart,
    ),
]
scala_implementation_list = [
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

DASK_STRATEGY_NAME_LIST = [x.strategy_name for x in dask_implementation_list]
PYSPARK_STRATEGY_NAME_LIST = [x.strategy_name for x in pyspark_implementation_list]
