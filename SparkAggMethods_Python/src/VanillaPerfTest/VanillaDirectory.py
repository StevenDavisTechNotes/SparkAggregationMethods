from typing import List

from PerfTestCommon import ExternalTestMethod
from SixFieldCommon.Dask_SixFieldTestData import DaskPythonTestMethod
from SixFieldCommon.PySpark_SixFieldTestData import PysparkPythonTestMethod
from VanillaPerfTest.Dask_Strategy.Da_VanillaPandas import da_vanilla_pandas
from VanillaPerfTest.PySpark_Strategy.VanillaFluent import vanilla_fluent
from VanillaPerfTest.PySpark_Strategy.VanillaPandas import vanilla_pandas
from VanillaPerfTest.PySpark_Strategy.VanillaPandasNumba import \
    vanilla_pandas_numba
from VanillaPerfTest.PySpark_Strategy.VanillaPandasNumpy import \
    vanilla_pandas_numpy
from VanillaPerfTest.PySpark_Strategy.VanillaRddGrpmap import \
    vanilla_rdd_grpmap
from VanillaPerfTest.PySpark_Strategy.VanillaRddMappart import \
    vanilla_rdd_mappart
from VanillaPerfTest.PySpark_Strategy.VanillaRddReduce import \
    vanilla_rdd_reduce
from VanillaPerfTest.PySpark_Strategy.VanillaSql import vanilla_sql

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
        strategy_name='vanilla_sql',
        language='python',
        interface='sql',
        delegate=vanilla_sql
    ),
    PysparkPythonTestMethod(
        strategy_name='vanilla_fluent',
        language='python',
        interface='sql',
        delegate=vanilla_fluent,
    ),
    PysparkPythonTestMethod(
        strategy_name='vanilla_pandas',
        language='python',
        interface='pandas',
        delegate=vanilla_pandas,
    ),
    PysparkPythonTestMethod(
        strategy_name='vanilla_pandas_numpy',
        language='python',
        interface='pandas',
        delegate=vanilla_pandas_numpy,
    ),
    PysparkPythonTestMethod(
        strategy_name='vanilla_pandas_numba',
        language='python',
        interface='pandas',
        delegate=vanilla_pandas_numba,
    ),
    PysparkPythonTestMethod(
        strategy_name='vanilla_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=vanilla_rdd_grpmap,
    ),
    PysparkPythonTestMethod(
        strategy_name='vanilla_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=vanilla_rdd_reduce,
    ),
    PysparkPythonTestMethod(
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
