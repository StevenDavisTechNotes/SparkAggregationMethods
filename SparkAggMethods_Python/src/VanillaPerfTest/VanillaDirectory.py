from typing import List

from SixFieldCommon.SixFieldTestData import PythonTestMethod
from PerfTestCommon import ExternalTestMethod

from VanillaPerfTest.PySpark_Strategy.VanillaFluent import vanilla_fluent
from VanillaPerfTest.PySpark_Strategy.VanillaPandas import vanilla_pandas
from VanillaPerfTest.PySpark_Strategy.VanillaPandasCuda import vanilla_panda_cupy
from VanillaPerfTest.PySpark_Strategy.VanillaPandasNumba import vanilla_pandas_numba
from VanillaPerfTest.PySpark_Strategy.VanillaPandasNumpy import vanilla_pandas_numpy
from VanillaPerfTest.PySpark_Strategy.VanillaRddGrpmap import vanilla_rdd_grpmap
from VanillaPerfTest.PySpark_Strategy.VanillaRddMappart import vanilla_rdd_mappart
from VanillaPerfTest.PySpark_Strategy.VanillaRddReduce import vanilla_rdd_reduce
from VanillaPerfTest.PySpark_Strategy.VanillaSql import vanilla_sql

implementation_list: List[PythonTestMethod] = [
    PythonTestMethod(
        strategy_name='vanilla_sql',
        language='python',
        interface='sql',
        delegate=vanilla_sql
    ),
    PythonTestMethod(
        strategy_name='vanilla_fluent',
        language='python',
        interface='sql',
        delegate=vanilla_fluent,
    ),
    PythonTestMethod(
        strategy_name='vanilla_pandas',
        language='python',
        interface='pandas',
        delegate=vanilla_pandas,
    ),
    PythonTestMethod(
        strategy_name='vanilla_pandas_numpy',
        language='python',
        interface='pandas',
        delegate=vanilla_pandas_numpy,
    ),
    PythonTestMethod(
        strategy_name='vanilla_panda_cupy',
        language='python',
        interface='panda',
        delegate=vanilla_panda_cupy,),
    PythonTestMethod(
        strategy_name='vanilla_pandas_numba',
        language='python',
        interface='pandas',
        delegate=vanilla_pandas_numba,
    ),
    PythonTestMethod(
        strategy_name='vanilla_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=vanilla_rdd_grpmap,
    ),
    PythonTestMethod(
        strategy_name='vanilla_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=vanilla_rdd_reduce,
    ),
    PythonTestMethod(
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

strategy_name_list = [x.strategy_name for x in implementation_list]
