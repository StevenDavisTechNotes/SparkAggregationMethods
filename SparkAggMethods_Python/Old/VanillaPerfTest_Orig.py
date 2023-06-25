#!python
# set PYSPARK_DRIVER_PYTHON=python
# set PYSPARK_DRIVER_PYTHON_OPTS=
# conda activate pyspark36
# prompt $g
# spark-submit --master local[8] --driver-memory 1g --deploy-mode client --conf spark.pyspark.virtualenv.enabled=true  --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.requirements=requirements.txt --conf spark.pyspark.virtualenv.bin.path=venv --conf spark.pyspark.python="$pwd\venv\scripts\python.exe" 'VanillaPerfTest.py'
# spark-submit --master local[8] --deploy-mode client 'VanillaPerfTest.py'
# spark-submit --master 'local[8]' --deploy-mode 'cluster'  --archives
# 'pyspark_venv.tar.gz#environment' 'VanillaPerfTest.py'
from typing import List, Callable, Tuple, Any, cast, Optional, Iterable

from dataclasses import dataclass
import datetime
import os
import math
import random
import collections
import gc
import time

import pandas as pd
from numba import vectorize, jit, njit, prange, cuda
from numba import float64 as numba_float64
import numpy

import findspark
from pyspark import SparkContext, RDD
from pyspark.rdd import PythonEvalType
from pyspark.sql import Row, SparkSession
from pyspark.sql.dataframe import DataFrame as spark_DataFrame
import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql.window import Window

from scipy.stats import norm as scipy_stats_norm  # type: ignore

from LinearRegression import linear_regression


def createSparkContext() -> SparkSession:
    findspark.init()
    full_path_to_python = os.path.join(
        os.getcwd(), "venv", "scripts", "python.exe")
    os.environ["PYSPARK_PYTHON"] = full_path_to_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = full_path_to_python
    spark = (
        SparkSession
        .builder
        .appName("VanillaPerfTest")
        .config("spark.sql.shuffle.partitions", 7)
        .config("spark.ui.enabled", "false")
        .config("spark.rdd.compress", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "3g")
        .config("spark.executor.memoryOverhead", "1g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.pyspark.virtualenv.enabled", "true")
        .config("spark.pyspark.virtualenv.type", "native")
        .config("spark.pyspark.virtualenv.requirements", "requirements.txt")
        .config("spark.pyspark.virtualenv.bin.path", "venv")
        .config("spark.pyspark.python", full_path_to_python)
        .getOrCreate()
    )
    return spark


def setupSparkContext(in_spark) -> Tuple[SparkContext, Any]:
    spark = in_spark
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.info("script initialized")
    return sc, log


@dataclass(frozen=True)
class DataPoint:
    id: int
    grp: int
    subgrp: int
    A: int
    B: int
    C: float
    D: float
    E: float
    F: float


# DataPoint = collections.namedtuple("DataPoint",
#                                    ["id", "grp", "subgrp", "A", "B", "C", "D", "E", "F"])
DataPointSchema = DataTypes.StructType([
    DataTypes.StructField('id', DataTypes.LongType(), False),
    DataTypes.StructField('grp', DataTypes.LongType(), False),
    DataTypes.StructField('subgrp', DataTypes.LongType(), False),
    DataTypes.StructField('A', DataTypes.LongType(), False),
    DataTypes.StructField('B', DataTypes.LongType(), False),
    DataTypes.StructField('C', DataTypes.DoubleType(), False),
    DataTypes.StructField('D', DataTypes.DoubleType(), False),
    DataTypes.StructField('E', DataTypes.DoubleType(), False),
    DataTypes.StructField('F', DataTypes.DoubleType(), False)])


def generateData(numGrp1=3, numGrp2=3, repetition=1000):
    return [
        DataPoint(
            id=i,
            grp=(i // numGrp2) % numGrp1,
            subgrp=i % numGrp2,
            A=random.randint(1, repetition),
            B=random.randint(1, repetition),
            C=random.uniform(1, 10),
            D=random.uniform(1, 10),
            E=random.normalvariate(0, 10),
            F=random.normalvariate(1, 10))
        for i in range(0, numGrp1 * numGrp2 * repetition)]


@dataclass(frozen=True)
class PythonTestMethod:
    name: str
    language: str
    interface: str
    delegate: Callable


@dataclass(frozen=True)
class ExternalTestMethod:
    name: str
    language: str
    interface: str


# CondMethod = collections.namedtuple("CondMethod",
#                                     ["name", "language", "interface", "delegate"])
implementation_list: List[PythonTestMethod] = []


# region Utilities

def count_iter(iterator):
    count = 0
    for _ in iterator:
        count += 1
    return count
# endregion
# region vanilla aggregation


def vanilla_sql(pyData):
    df = spark.createDataFrame(pyData, schema=DataPointSchema)
    spark.catalog.dropTempView("exampledata")
    df.createTempView("exampledata")
    df = spark.sql('''
    SELECT
        grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D,
        VAR_SAMP(E) var_of_E,
        (
            SUM(E*E) -
            SUM(E) * SUM(E) / COUNT(E)
        ) / (COUNT(E) - 1) var_of_E2
    FROM
        exampledata
    GROUP BY grp, subgrp
    ORDER BY grp, subgrp
    ''')
    return None, df


implementation_list.append(PythonTestMethod(
    name='vanilla_sql',
    language='python',
    interface='sql',
    delegate=lambda pyData: vanilla_sql(pyData)))


def vanilla_fluent(pyData):
    df = spark.createDataFrame(pyData, schema=DataPointSchema)
    df = df \
        .groupBy(df.grp, df.subgrp) \
        .agg(
            func.mean(df.C).alias("mean_of_C"),
            func.max(df.D).alias("max_of_D"),
            func.variance(df.E).alias("var_of_E"),
            ((
                func.sum(df.E * df.E)
                - func.pow(func.sum(df.E), 2) / func.count(df.E)
            ) / (func.count(df.E) - 1)).alias("var_of_E2")
        )\
        .orderBy(df.grp, df.subgrp)
    return None, df


implementation_list.append(PythonTestMethod(
    name='vanilla_fluent',
    language='python',
    interface='sql',
    delegate=lambda pyData: vanilla_fluent(pyData)))


def vanilla_pandas(pyData) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    groupby_columns = ['grp', 'subgrp']
    agg_columns = ['mean_of_C', 'max_of_D', 'var_of_E', 'var_of_E2']
    df = spark.createDataFrame(pyData, schema=DataPointSchema)
    postAggSchema = DataTypes.StructType(
        [x for x in DataPointSchema.fields if x.name in groupby_columns] +
        [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in agg_columns])
    #

    # @pandas_udf_df_to_df(postAggSchema)
    def inner_agg_method(dfPartition: pd.DataFrame) -> pd.DataFrame:
        group_key = dfPartition['grp'].iloc[0]
        subgroup_key = dfPartition['subgrp'].iloc[0]
        C = dfPartition['C']
        D = dfPartition['D']
        E = dfPartition['E']
        return pd.DataFrame([[
            group_key,
            subgroup_key,
            C.mean(),
            D.max(),
            E.var(),
            ((E * E).sum() - E.sum()**2 / E.count()) / (E.count() - 1),
        ]], columns=groupby_columns + agg_columns)
    #
    aggregates = (
        df.groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    return None, aggregates


implementation_list.append(PythonTestMethod(
    name='vanilla_pandas',
    language='python',
    interface='pandas',
    delegate=lambda pyData: vanilla_pandas(pyData)))


def vanilla_pandas_numpy(pyData):
    groupby_columns = ['grp', 'subgrp']
    agg_columns = ['mean_of_C', 'max_of_D', 'var_of_E', 'var_of_E2']
    df = spark.createDataFrame(pyData, schema=DataPointSchema)
    postAggSchema = DataTypes.StructType(
        [x for x in DataPointSchema.fields if x.name in groupby_columns] +
        [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in agg_columns])
    #

    def inner_agg_method(dfPartition: pd.DataFrame) -> pd.DataFrame:
        group_key = dfPartition['grp'].iloc[0]
        subgroup_key = dfPartition['subgrp'].iloc[0]
        C = dfPartition['C']
        D = dfPartition['D']
        E = dfPartition['E']
        return pd.DataFrame([[
            group_key,
            subgroup_key,
            numpy.mean(C),
            numpy.max(D),
            numpy.var(E),
            (numpy.inner(E, E) - numpy.sum(E)**2 / E.count()) / (E.count() - 1),
        ]], columns=groupby_columns + agg_columns)
    #
    aggregates = (
        df.groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    return None, aggregates


implementation_list.append(PythonTestMethod(
    name='vanilla_pandas_numpy',
    language='python',
    interface='pandas',
    delegate=lambda pyData: vanilla_pandas_numpy(pyData)))

# def vanilla_panda_cupy(pyData):
#     groupby_columns = ['grp', 'subgrp']
#     agg_columns = ['mean_of_C','max_of_D', 'var_of_E', 'var_of_E2']
#     df = spark.createDataFrame(pyData, schema=DataPointSchema)
#     postAggSchema = DataTypes.StructType(
#         [x for x in DataPointSchema.fields if x.name in groupby_columns] +
#         [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in agg_columns])
#     #
#     @pandas_udf(postAggSchema, PandasUDFType.GROUPED_MAP)
#     def inner_agg_method(dfPartition):
#         group_key = dfPartition['grp'].iloc[0]
#         subgroup_key = dfPartition['subgrp'].iloc[0]
#         C = cupy.asarray(dfPartition['C'])
#         D = cupy.asarray(dfPartition['D'])
#         pdE = dfPartition['E']
#         E = cupy.asarray(pdE)
#         nE = pdE.count()
#         return pd.DataFrame([[
#             group_key,
#             subgroup_key,
#             np.float(cupy.asnumpy(cupy.mean(C))),
#             np.float(cupy.asnumpy(cupy.max(D))),
#             np.float(cupy.asnumpy(cupy.var(E))),
#             np.float(cupy.asnumpy((cupy.inner(E,E) - cupy.sum(E)**2/nE)/(nE-1))),
#             ]], columns=groupby_columns + agg_columns)
#     #
#     aggregates = df.groupby(df.grp, df.subgrp).apply(inner_agg_method)
#     return None, aggregates

# implementation_list.append(CondMethod(
#     name='vanilla_panda_cupy',
#     language='python',
#     interface='panda',
#     delegate=lambda pyData: vanilla_panda_cupy(pyData)))


def vanilla_pandas_numba(pyData):
    groupby_columns = ['grp', 'subgrp']
    agg_columns = ['mean_of_C', 'max_of_D', 'var_of_E', 'var_of_E2']
    df = spark.createDataFrame(pyData, schema=DataPointSchema)
    postAggSchema = DataTypes.StructType(
        [x for x in DataPointSchema.fields if x.name in groupby_columns] +
        [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in agg_columns])
    #

    @jit(numba_float64(numba_float64[:]), nopython=True)
    def my_numba_mean(C):
        return numpy.mean(C)
    #

    @jit(numba_float64(numba_float64[:]), nopython=True)
    def my_numba_max(C):
        return numpy.max(C)
    #

    @jit(numba_float64(numba_float64[:]), nopython=True)
    def my_numba_var(C):
        return numpy.var(C)
    #

    @jit(numba_float64(numba_float64[:]), parallel=True, nopython=True)
    def my_looplift_var(E):
        n = len(E)
        accE2 = 0.
        for i in prange(n):
            accE2 += E[i] ** 2
        accE = 0.
        for i in prange(n):
            accE += E[i]
        return (accE2 - accE**2 / n) / (n - 1)
    #

    # @pandas_udf(postAggSchema, PandasUDFType.GROUPED_MAP)
    def inner_agg_method(dfPartition):
        group_key = dfPartition['grp'].iloc[0]
        subgroup_key = dfPartition['subgrp'].iloc[0]
        C = numpy.array(dfPartition['C'])
        D = numpy.array(dfPartition['D'])
        E = numpy.array(dfPartition['E'])
        return pd.DataFrame([[
            group_key,
            subgroup_key,
            my_numba_mean(C),
            my_numba_max(D),
            my_numba_var(E),
            my_looplift_var(E),
        ]], columns=groupby_columns + agg_columns)
    #
    aggregates = (
        df.groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    return None, aggregates


implementation_list.append(PythonTestMethod(
    name='vanilla_pandas_numba',
    language='python',
    interface='pandas',
    delegate=lambda pyData: vanilla_pandas_numba(pyData)))
# endregion

# region vanilla grpMap


def vanilla_rdd_grpmap(pyData):
    rddData = sc.parallelize(pyData)

    def processData1(key, iterator) -> Tuple[Tuple[int, int], Row]:
        import math
        sum_of_C = 0
        count = 0
        max_of_D = None
        sum_of_E_squared = 0
        sum_of_E = 0
        for item in iterator:
            sum_of_C = sum_of_C + item.C
            count = count + 1
            max_of_D = item.D \
                if max_of_D is None or max_of_D < item.D \
                else max_of_D
            sum_of_E_squared += item.E * item.E
            sum_of_E += item.E
        mean_of_C = sum_of_C / count \
            if count > 0 else math.nan
        var_of_E = math.nan
        if count >= 2:
            var_of_E = \
                (
                    sum_of_E_squared
                    - sum_of_E * sum_of_E / count
                ) / (count - 1)
        return (key,
                Row(grp=key[0],
                    subgrp=key[1],
                    mean_of_C=mean_of_C,
                    max_of_D=max_of_D,
                    var_of_E=var_of_E))

    rddProcessed: RDD[Tuple[Tuple[int, int], Iterable[Row]]] = (
        rddData
        .groupBy(lambda x: (x.grp, x.subgrp))
        .map(lambda pair: processData1(pair[0], pair[1]))
    )
    rddResult = (
        cast(Any, rddProcessed)
        .coalesce(numPartitions=1)
        .sortByKey()
        .values()
    )
    return rddResult, None


implementation_list.append(PythonTestMethod(
    name='vanilla_rdd_grpmap',
    language='python',
    interface='rdd',
    delegate=lambda pyData: vanilla_rdd_grpmap(pyData)))
# endregion

# region vanilla reduce


def vanilla_rdd_reduce(pyData):

    @dataclass(frozen=True)
    class SubTotal:
        running_sum_of_C: float
        running_count: int
        running_max_of_D: Optional[float]
        running_sum_of_E_squared: float
        running_sum_of_E: float

    rddData = sc.parallelize(pyData)
    # SubTotal = collections.namedtuple("SubTotal",
    #                                   ["running_sum_of_C", "running_count", "running_max_of_D",
    #                                    "running_sum_of_E_squared", "running_sum_of_E"])

    def mergeValue2(sub, v):
        running_sum_of_C = sub.running_sum_of_C + v.C
        running_count = sub.running_count + 1
        running_max_of_D = sub.running_max_of_D \
            if sub.running_max_of_D is not None and \
            sub.running_max_of_D > v.D \
            else v.D
        running_sum_of_E_squared = sub.running_sum_of_E_squared
        running_sum_of_E = sub.running_sum_of_E
        running_sum_of_E_squared += v.E * v.E
        running_sum_of_E += v.E
        return SubTotal(
            running_sum_of_C,
            running_count,
            running_max_of_D,
            running_sum_of_E_squared,
            running_sum_of_E)

    def createCombiner2(v):
        return mergeValue2(SubTotal(
            running_sum_of_C=0,
            running_count=0,
            running_max_of_D=None,
            running_sum_of_E_squared=0,
            running_sum_of_E=0), v)

    def mergeCombiners2(lsub, rsub):
        return SubTotal(
            running_sum_of_C=lsub.running_sum_of_C + rsub.running_sum_of_C,
            running_count=lsub.running_count + rsub.running_count,
            running_max_of_D=lsub.running_max_of_D
            if lsub.running_max_of_D is not None and
            lsub.running_max_of_D > rsub.running_max_of_D
            else rsub.running_max_of_D,
            running_sum_of_E_squared=lsub.running_sum_of_E_squared +
            rsub.running_sum_of_E_squared,
            running_sum_of_E=lsub.running_sum_of_E + rsub.running_sum_of_E)

    def finalAnalytics2(key, total):
        sum_of_C = total.running_sum_of_C
        count = total.running_count
        max_of_D = total.running_max_of_D
        sum_of_E_squared = total.running_sum_of_E_squared
        sum_of_E = total.running_sum_of_E
        return Row(
            grp=key[0], subgrp=key[1],
            mean_of_C=math.nan
            if count < 1 else sum_of_C / count,
            max_of_D=max_of_D,
            var_of_E=math.nan
            if count < 2 else
            (
                sum_of_E_squared -
                sum_of_E * sum_of_E / count
            ) / (count - 1))

    sumCount: RDD[Row] = (
        rddData
        .map(lambda x: ((x.grp, x.subgrp), x))
        .combineByKey(createCombiner2,
                      mergeValue2,
                      mergeCombiners2)
        .map(lambda kv: finalAnalytics2(kv[0], kv[1])))
    rddResult = cast(Any, sumCount).sortBy(lambda x: (x.grp, x.subgrp))
    return rddResult, None


implementation_list.append(PythonTestMethod(
    name='vanilla_rdd_reduce',
    language='python',
    interface='rdd',
    delegate=lambda pyData: vanilla_rdd_reduce(pyData)))
# endregion

# region vanilla mapPartitions


def vanilla_rdd_mappart(pyData):

    @dataclass(frozen=True)
    class SubTotal:
        running_sum_of_C: float
        running_count: int
        running_max_of_D: Optional[float]
        running_sum_of_E_squared: float
        running_sum_of_E: float

    rddData = sc.parallelize(pyData)
    # SubTotal = collections.namedtuple("SubTotal",
    #                                   ["running_sum_of_C", "running_count", "running_max_of_D",
    #                                    "running_sum_of_E_squared", "running_sum_of_E"])

    class MutableRunningTotal:
        def __init__(self):
            self.running_sum_of_C = 0
            self.running_count = 0
            self.running_max_of_D = None
            self.running_sum_of_E_squared = 0
            self.running_sum_of_E = 0

    def partitionTriage(iterator):
        running_subtotals = {}
        for v in iterator:
            k = (v.grp, v.subgrp)
            if k not in running_subtotals:
                running_subtotals[k] = MutableRunningTotal()
            sub = running_subtotals[k]
            sub.running_sum_of_C += v.C
            sub.running_count += 1
            sub.running_max_of_D = \
                sub.running_max_of_D \
                if sub.running_max_of_D is not None and \
                sub.running_max_of_D > v.D \
                else v.D
            sub.running_sum_of_E_squared += v.E * v.E
            sub.running_sum_of_E += v.E
        for k in running_subtotals:
            sub = running_subtotals[k]
            yield (k, SubTotal(
                running_sum_of_C=sub.running_sum_of_C,
                running_count=sub.running_count,
                running_max_of_D=sub.running_max_of_D,
                running_sum_of_E_squared=sub.running_sum_of_E_squared,
                running_sum_of_E=sub.running_sum_of_E))

    def mergeCombiners3(key, iterable):
        lsub = MutableRunningTotal()
        for rsub in iterable:
            lsub.running_sum_of_C += rsub.running_sum_of_C
            lsub.running_count += rsub.running_count
            lsub.running_max_of_D = lsub.running_max_of_D \
                if lsub.running_max_of_D is not None and \
                lsub.running_max_of_D > rsub.running_max_of_D \
                else rsub.running_max_of_D
            lsub.running_sum_of_E_squared += \
                rsub.running_sum_of_E_squared
            lsub.running_sum_of_E += rsub.running_sum_of_E
        return SubTotal(
            running_sum_of_C=lsub.running_sum_of_C,
            running_count=lsub.running_count,
            running_max_of_D=lsub.running_max_of_D,
            running_sum_of_E_squared=lsub.running_sum_of_E_squared,
            running_sum_of_E=lsub.running_sum_of_E)

    def finalAnalytics2(key, final):
        sum_of_C = final.running_sum_of_C
        count = final.running_count
        max_of_D = final.running_max_of_D
        sum_of_E_squared = final.running_sum_of_E_squared
        sum_of_E = final.running_sum_of_E
        return Row(
            grp=key[0], subgrp=key[1],
            mean_of_C=math.nan
            if count < 1 else
            sum_of_C / count,
            max_of_D=max_of_D,
            var_of_E=math.nan
            if count < 2 else
            (
                sum_of_E_squared -
                sum_of_E * sum_of_E / count
            ) / (count - 1))

    sumCount = rddData \
        .mapPartitions(partitionTriage) \
        .groupByKey() \
        .map(lambda kv: (kv[0], mergeCombiners3(kv[0], kv[1]))) \
        .map(lambda kv: finalAnalytics2(kv[0], kv[1]))
    rddResult = cast(Any, sumCount).sortBy(lambda x: (x.grp, x.subgrp))
    # df = spark.createDataFrame(rddResult)\
    #     .select('grp', 'subgrp', 'mean_of_C', 'max_of_D', 'var_of_E')
    # return df.rdd, None
    return rddResult, None


implementation_list.append(PythonTestMethod(
    name='vanilla_rdd_mappart',
    language='python',
    interface='rdd',
    delegate=lambda pyData: vanilla_rdd_mappart(pyData)))
# endregion


@dataclass(frozen=True)
class RunResult:
    dataSize: int
    elapsedTime: float
    recordCount: int

# RunResult = collections.namedtuple("RunResult", ["dataSize", "elapsedTime"])


def DoTesting(spark: SparkSession, sc: SparkContext, log):
    pyData_3_3_1 = generateData(3, 3, 10**0)
    pyData_3_3_10 = generateData(3, 3, 10**1)
    pyData_3_3_100 = generateData(3, 3, 10**2)
    pyData_3_3_1k = generateData(3, 3, 10**3)
    pyData_3_3_10k = generateData(3, 3, 10**4)
    pyData_3_3_100k = generateData(3, 3, 10**5)
    # pyData_3_3_1m = generateData(3,3,10**6)
    NumRunsPer = 30  # 100
    cond_run_itinerary = []
    for cond_method in implementation_list:
        for data in [pyData_3_3_10, pyData_3_3_100,
                     pyData_3_3_1k, pyData_3_3_10k, pyData_3_3_100k]:
            cond_run_itinerary.extend((cond_method, data)
                                      for _i in range(0, NumRunsPer))
    # random.shuffle(cond_run_itinerary)
    #
    # vanilla_panda_cupy(pyData_3_3_1) # for code generation
    with open('Results/vanilla_runs.csv', 'a') as f:
        for index, (cond_method, data) in enumerate(cond_run_itinerary):
            log.info("Working on %d of %d" % (index, len(cond_run_itinerary)))
            startedTime = time.time()
            rdd, df = cond_method.delegate(data)
            if df is not None:
                rdd = df.rdd
            recordCount = count_iter(rdd.toLocalIterator())
            finishedTime = time.time()
            result = RunResult(
                dataSize=len(data),
                elapsedTime=finishedTime - startedTime,
                recordCount=recordCount)
            f.write("%s,%s,%d,%f,%d\n" % (cond_method.name, cond_method.interface,
                    result.dataSize, result.elapsedTime, result.recordCount))
            f.flush()
            df = None
            rdd = None
            gc.collect()


def DoAnalysis(python_implementation_list):
    import scipy.stats
    from LinearRegression import linear_regression
    cond_runs = {}
    with open('Results/vanilla_runs.csv', 'r') as f:
        for textline in f:
            if textline.startswith('#'):
                # print("Excluding line: "+textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: " + textline)
                continue
            fields = textline.rstrip().split(',')
            if len(fields) < 5:
                fields.append('9')
            cond_method_name, cond_method_interface, result_dataSize, result_elapsedTime, result_recordCount = tuple(
                fields)
            if result_recordCount != '9':
                print("Excluding line: " + textline)
                continue
            typed_method_name = f"{cond_method_name}_python"
            if typed_method_name not in cond_runs:
                cond_runs[typed_method_name] = []
            result = RunResult(
                dataSize=int(result_dataSize),
                elapsedTime=float(result_elapsedTime),
                recordCount=int(result_recordCount))
            cond_runs[typed_method_name].append(result)
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
    with open('../Results/Scala/vanilla_runs_scala.csv', 'r') as f:
        for textline in f:
            if textline.startswith('#'):
                print("Excluding line: " + textline)
                continue
            if textline.startswith(' '):
                print("Excluding line: " + textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: " + textline)
                continue
            fields = textline.rstrip().split(',')
            outcome, rawmethod, interface, expectedSize, returnedSize, elapsedTime = tuple(
                fields)
            if outcome != 'success':
                print("Excluding line: " + textline)
                continue
            if returnedSize != '9':
                print("Excluding line: " + textline)
                continue
            typed_method_name = f"{rawmethod}_scala"
            if typed_method_name not in cond_runs:
                cond_runs[typed_method_name] = []
            result = RunResult(
                dataSize=int(expectedSize),
                elapsedTime=float(elapsedTime),
                recordCount=-1)
            cond_runs[typed_method_name].append(result)
    #
    CondResult = collections.namedtuple("CondResult",
                                        ["name", "interface",
                                         "b0", "b0_low", "b0_high",
                                         "b1", "b1_low", "b1_high",
                                         "s2", "s2_low", "s2_high"])
    summary_status = ''
    regression_status = ''
    if True:
        FullCondMethod = collections.namedtuple("FullCondMethod",
                                                ["data_name", "raw_method_name", "language", "interface"])
        cond_results = []
        confidence = 0.95
        sorted_implementation_list = sorted(
            [FullCondMethod(
                data_name=f"{x.name}_{x.language}", raw_method_name=x.name,
                language=x.language, interface=x.interface)
                for x in python_implementation_list + scala_implementation_list],
            key=lambda x: (x.language, x.interface, x.raw_method_name))
        summary_status += ",".join([
            'RunName', 'RawMethod', 'Method', 'Language', 'Interface',
            'DataSize', 'NumRuns', 'Elapsed Time', 'stdev', 'rl', 'rh']) + "\n"
        for method in sorted_implementation_list:
            times = cond_runs[method.data_name]
            size_values = set(x.dataSize for x in times)
            for dataSize in sorted(size_values):
                ar = [x.elapsedTime for x in times if x.dataSize == dataSize]
                numRuns = len(ar)
                mean = numpy.mean(ar)
                stdev = numpy.std(ar, ddof=1)
                rl, rh = scipy_stats_norm.interval(
                    confidence, loc=mean, scale=stdev / math.sqrt(len(ar)))
                summary_status += "%s,%s,%s,%s,%s,%d,%d,%f,%f,%f,%f\n" % (
                    method.data_name,
                    method.raw_method_name,
                    method.raw_method_name.replace("vanilla_", ""),
                    method.language,
                    method.interface,
                    dataSize, numRuns, mean, stdev, rl, rh
                )
    with open('Results/vanilla_results_20180930.csv', 'wt') as f:
        f.write(summary_status)
        f.write("\n")


#
if __name__ == "__main__":
    spark = createSparkContext()
    sc, log = setupSparkContext(spark)
    DoTesting(spark, sc, log)
    # DoAnalysis(implementation_list)
