from typing import List

from challenges.vanilla.using_dask.vanilla_dask_grp_pandas import \
    da_vanilla_pandas
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
from challenges.vanilla.using_pyspark.vanilla_pyspark_sql import vanilla_sql
from perf_test_common import ExternalTestMethod
from six_field_test_data.six_generate_test_data_using_dask import \
    DaskPythonTestMethod
from six_field_test_data.six_generate_test_data_using_pyspark import \
    PysparkPythonTestMethod
from utils.inspection import nameof

dask_implementation_list: List[DaskPythonTestMethod] = [
    DaskPythonTestMethod(
        strategy_name=nameof(da_vanilla_pandas),
        language='python',
        interface='da_pandas',
        delegate=da_vanilla_pandas,
    ),
]

pyspark_implementation_list: List[PysparkPythonTestMethod] = [
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_sql',
        strategy_name=nameof(vanilla_sql),
        language='python',
        interface='sql',
        delegate=vanilla_sql
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_fluent',
        strategy_name=nameof(vanilla_pyspark_df_grp_builtin),
        language='python',
        interface='sql',
        delegate=vanilla_pyspark_df_grp_builtin,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_pandas',
        strategy_name=nameof(vanilla_pyspark_df_grp_pandas),
        language='python',
        interface='pandas',
        delegate=vanilla_pyspark_df_grp_pandas,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_pandas_numpy',
        strategy_name=nameof(vanilla_pyspark_df_grp_pandas_numpy),
        language='python',
        interface='pandas',
        delegate=vanilla_pyspark_df_grp_pandas_numpy,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_pandas_numba',
        strategy_name=nameof(vanilla_pyspark_df_grp_pandas_numba),
        language='python',
        interface='pandas',
        delegate=vanilla_pyspark_df_grp_pandas_numba,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_rdd_grpmap',
        strategy_name=nameof(vanilla_pyspark_rdd_grp_map),
        language='python',
        interface='rdd',
        delegate=vanilla_pyspark_rdd_grp_map,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_rdd_reduce',
        strategy_name=nameof(vanilla_pyspark_rdd_reduce),
        language='python',
        interface='rdd',
        delegate=vanilla_pyspark_rdd_reduce,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_rdd_mappart',
        strategy_name=nameof(vanilla_pyspark_rdd_mappart),
        language='python',
        interface='rdd',
        delegate=vanilla_pyspark_rdd_mappart,
    ),
]
scala_implementation_list = [
    ExternalTestMethod(
        strategy_name='vanilla_sql',
        language='scala',
        interface='sql'),
    ExternalTestMethod(
        strategy_name='vanilla_fluent',
        language='scala',
        interface='sql'),
    ExternalTestMethod(
        strategy_name='vanilla_udaf',
        language='scala',
        interface='sql'),
    ExternalTestMethod(
        strategy_name='vanilla_rdd_grpmap',
        language='scala',
        interface='rdd'),
    ExternalTestMethod(
        strategy_name='vanilla_rdd_reduce',
        language='scala',
        interface='rdd'),
    ExternalTestMethod(
        strategy_name='vanilla_rdd_mappart',
        language='scala',
        interface='rdd'),
]

dask_strategy_name_list = [x.strategy_name for x in dask_implementation_list]
pyspark_strategy_name_list = [x.strategy_name for x in pyspark_implementation_list]
