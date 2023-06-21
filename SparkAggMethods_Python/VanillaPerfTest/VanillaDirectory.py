from typing import List, Callable, Tuple, Optional
from dataclasses import dataclass

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from PerfTestCommon import ExternalTestMethod
from Utils.SparkUtils import TidySparkSession

from .VanillaTestData import DataPoint
from .Strategy.VanillaSql import vanilla_sql
from .Strategy.VanillaFluent import vanilla_fluent
from .Strategy.VanillaPandas import vanilla_pandas
from .Strategy.VanillaPandasNumpy import vanilla_pandas_numpy
from .Strategy.VanillaPandasCuda import vanilla_panda_cupy
from .Strategy.VanillaPandasNumba import vanilla_pandas_numba
from .Strategy.VanillaRddGrpmap import vanilla_rdd_grpmap
from .Strategy.VanillaRddReduce import vanilla_rdd_reduce
from .Strategy.VanillaRddMappart import vanilla_rdd_mappart


@dataclass(frozen=True)
class PythonTestMethod:
    name: str
    language: str
    interface: str
    delegate: Callable[
        [TidySparkSession, List[DataPoint]],
        Tuple[Optional[RDD], Optional[spark_DataFrame]]]


implementation_list: List[PythonTestMethod] = [
    PythonTestMethod(
        name='vanilla_sql',
        language='python',
        interface='sql',
        delegate=vanilla_sql
    ),
    PythonTestMethod(
        name='vanilla_fluent',
        language='python',
        interface='sql',
        delegate=vanilla_fluent,
    ),
    PythonTestMethod(
        name='vanilla_pandas',
        language='python',
        interface='pandas',
        delegate=vanilla_pandas,
    ),
    PythonTestMethod(
        name='vanilla_pandas_numpy',
        language='python',
        interface='pandas',
        delegate=vanilla_pandas_numpy,
    ),
    PythonTestMethod(
        name='vanilla_panda_cupy',
        language='python',
        interface='panda',
        delegate=vanilla_panda_cupy,),
    PythonTestMethod(
        name='vanilla_pandas_numba',
        language='python',
        interface='pandas',
        delegate=vanilla_pandas_numba,
    ),
    PythonTestMethod(
        name='vanilla_rdd_grpmap',
        language='python',
        interface='rdd',
        delegate=vanilla_rdd_grpmap,
    ),
    PythonTestMethod(
        name='vanilla_rdd_reduce',
        language='python',
        interface='rdd',
        delegate=vanilla_rdd_reduce,
    ),
    PythonTestMethod(
        name='vanilla_rdd_mappart',
        language='python',
        interface='rdd',
        delegate=vanilla_rdd_mappart,
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
