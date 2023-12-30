from typing import List

from challenges.vanilla.Dask_Strategy.Dask_VanillaPandas import \
    da_vanilla_pandas
from challenges.vanilla.PySpark_Strategy.VanillaDfFluent import \
    vanilla_df_fluent
from challenges.vanilla.PySpark_Strategy.VanillaDfGrpPandas import \
    vanilla_df_grp_pandas
from challenges.vanilla.PySpark_Strategy.VanillaDfGrpPandasNumba import \
    vanilla_df_grp_pandas_numba
from challenges.vanilla.PySpark_Strategy.VanillaDfGrpPandasNumpy import \
    vanilla_df_grp_pandas_numpy
from challenges.vanilla.PySpark_Strategy.VanillaRddGrpmap import \
    vanilla_rdd_grpmap
from challenges.vanilla.PySpark_Strategy.VanillaRddMappart import \
    vanilla_rdd_mappart
from challenges.vanilla.PySpark_Strategy.VanillaRddReduce import \
    vanilla_rdd_reduce
from challenges.vanilla.PySpark_Strategy.VanillaSql import vanilla_sql
from perf_test_common import ExternalTestMethod
from six_field_test_data.six_generate_test_data_using_dask import \
    DaskPythonTestMethod
from six_field_test_data.six_generate_test_data_using_pyspark import \
    PysparkPythonTestMethod

dask_implementation_list: List[DaskPythonTestMethod] = [
    DaskPythonTestMethod(
        strategy_name='da_vanilla_pandas',
        language='python',
        interface='da_pandas',
        delegate=da_vanilla_pandas,
    ),
]

pyspark_implementation_list: List[PysparkPythonTestMethod] = [
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_sql',
        strategy_name='vanilla_sql',
        language='python',
        interface='sql',
        delegate=vanilla_sql
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_fluent',
        strategy_name='vanilla_df_fluent',
        language='python',
        interface='sql',
        delegate=vanilla_df_fluent,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_pandas',
        strategy_name='vanilla_df_grp_pandas',
        language='python',
        interface='pandas',
        delegate=vanilla_df_grp_pandas,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_pandas_numpy',
        strategy_name='vanilla_df_grp_pandas_numpy',
        language='python',
        interface='pandas',
        delegate=vanilla_df_grp_pandas_numpy,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_pandas_numba',
        strategy_name='vanilla_df_grp_pandas_numba',
        language='python',
        interface='pandas',
        delegate=vanilla_df_grp_pandas_numba,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_rdd_grpmap',
        strategy_name='vanilla_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=vanilla_rdd_grpmap,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_rdd_reduce',
        strategy_name='vanilla_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=vanilla_rdd_reduce,
    ),
    PysparkPythonTestMethod(
        original_strategy_name='vanilla_rdd_mappart',
        strategy_name='vanilla_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=vanilla_rdd_mappart,
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
