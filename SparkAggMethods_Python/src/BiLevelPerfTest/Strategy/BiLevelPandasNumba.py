from typing import Optional, Tuple

import numpy as np
import pandas as pd
from numba import float64 as numba_float64
from numba import jit, prange
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession

from BiLevelPerfTest.BiLevelDataTypes import postAggSchema, result_columns


def bi_pandas_numba(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = data_set.data.dfSrc
    df = (
        df
        .groupBy(df.grp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    df = df.orderBy(df.grp)
    return None, df


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_mean(C: np.ndarray) -> np.float64:
    return np.mean(C)


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_max(D: np.ndarray) -> np.float64:
    return np.max(D)


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_var(E: np.ndarray) -> np.float64:
    return np.var(E)


@jit(numba_float64(numba_float64[:]), parallel=True, nopython=True)
def my_looplift_var(E: np.ndarray) -> float:
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
    C = np.array(dfPartition['C'])
    D = np.array(dfPartition['D'])
    subgroupedE = dfPartition.groupby('subgrp')['E']
    return pd.DataFrame([[
        group_key,
        my_numba_mean(C),
        my_numba_max(D),
        subgroupedE.apply(lambda x: my_numba_var(np.array(x))).mean(),
        subgroupedE.apply(lambda x: my_looplift_var(np.array(x))).mean(),
    ]], columns=result_columns)
