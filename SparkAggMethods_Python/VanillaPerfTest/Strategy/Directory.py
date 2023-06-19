from typing import List

from PerfTestCommon import (
    PythonTestMethod, ExternalTestMethod
)
from VanillaPerfTest.Strategy.VanillaSql import vanilla_sql
from VanillaPerfTest.Strategy.VanillaFluent import vanilla_fluent
from VanillaPerfTest.Strategy.VanillaPandas import vanilla_pandas
from VanillaPerfTest.Strategy.VanillaPandasNumpy import vanilla_pandas_numpy
from VanillaPerfTest.Strategy.VanillaPandasCuda import vanilla_panda_cupy
from VanillaPerfTest.Strategy.VanillaPandasNumba import vanilla_pandas_numba
from VanillaPerfTest.Strategy.VanillaRddGrpmap import vanilla_rdd_grpmap
from VanillaPerfTest.Strategy.VanillaRddReduce import vanilla_rdd_reduce
from VanillaPerfTest.Strategy.VanillaRddMappart import vanilla_rdd_mappart

implementation_list: List[PythonTestMethod] = [
    PythonTestMethod(
        name='vanilla_sql',
        language='python',
        interface='sql',
        delegate=lambda spark, pyData: vanilla_sql(spark, pyData)),
    PythonTestMethod(
        name='vanilla_fluent',
        language='python',
        interface='sql',
        delegate=lambda spark, pyData: vanilla_fluent(spark, pyData)
    ),
    PythonTestMethod(
        name='vanilla_pandas',
        language='python',
        interface='pandas',
        delegate=lambda spark, pyData: vanilla_pandas(spark, pyData)
    ),
    PythonTestMethod(
        name='vanilla_pandas_numpy',
        language='python',
        interface='pandas',
        delegate=lambda spark, pyData: vanilla_pandas_numpy(spark, pyData)
    ),
    PythonTestMethod(
        name='vanilla_panda_cupy',
        language='python',
        interface='panda',
        delegate=lambda spark, pyData: vanilla_panda_cupy(spark, pyData)),
    PythonTestMethod(
        name='vanilla_pandas_numba',
        language='python',
        interface='pandas',
        delegate=lambda spark, pyData: vanilla_pandas_numba(spark, pyData)
    ),
    PythonTestMethod(
        name='vanilla_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=lambda spark, pyData: vanilla_rdd_grpmap(spark, pyData)
    ),
    PythonTestMethod(
        name='vanilla_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=lambda spark, pyData: vanilla_rdd_reduce(spark, pyData)
    ),
    PythonTestMethod(
        name='vanilla_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=lambda spark, pyData: vanilla_rdd_mappart(spark, pyData)
    ),
]
scala_implementation_list = [
    ExternalTestMethod(
        name='vanilla_sql',
        language='scala',
        interface='sql'),
    ExternalTestMethod(
        name='vanilla_fluent',
        language='scala',
        interface='sql'),
    ExternalTestMethod(
        name='vanilla_udaf',
        language='scala',
        interface='sql'),
    ExternalTestMethod(
        name='vanilla_rdd_grpmap',
        language='scala',
        interface='rdd'),
    ExternalTestMethod(
        name='vanilla_rdd_reduce',
        language='scala',
        interface='rdd'),
    ExternalTestMethod(
        name='vanilla_rdd_mappart',
        language='scala',
        interface='rdd'),
]
