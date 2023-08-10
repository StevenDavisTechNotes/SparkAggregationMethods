from typing import Optional, Tuple

import numpy
import pandas as pd
from numba import float64 as numba_float64
from numba import jit, prange
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.TidySparkSession import TidySparkSession

from VanillaPerfTest.VanillaDataTypes import postAggSchema, result_columns


def vanilla_pandas_numba(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = data_set.data.dfSrc

    df = (
        df.groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema)
        .orderBy(df.grp, df.subgrp)
    )
    return None, df


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_mean(C: numpy.ndarray) -> numpy.float64:
    return numpy.mean(C)


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_max(C: numpy.ndarray) -> numpy.float64:
    return numpy.max(C)


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_var(C: numpy.ndarray) -> numpy.float64:
    return numpy.var(C)


@jit(numba_float64(numba_float64[:]), parallel=True, nopython=True)
def my_looplift_var(E: numpy.ndarray) -> float:
    n = len(E)
    accE2 = 0.
    for i in prange(n):
        accE2 += E[i] ** 2
    accE = 0.
    for i in prange(n):
        accE += E[i]
    return accE2 / n - (accE / n)**2


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
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
    ]], columns=result_columns)
