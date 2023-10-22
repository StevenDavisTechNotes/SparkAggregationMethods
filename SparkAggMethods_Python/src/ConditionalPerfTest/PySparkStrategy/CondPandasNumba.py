from typing import Tuple

import numpy as np
import pandas as pd
from numba import float64 as numba_float64
from numba import jit, prange
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from ConditionalPerfTest.CondDataTypes import (agg_columns_4, groupby_columns,
                                               postAggSchema_4)
from SixFieldCommon.PySpark_SixFieldTestData import PysparkDataSet
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def cond_pandas_numba(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet,
) -> Tuple[RDD | None, spark_DataFrame | None]:
    df = data_set.data.dfSrc

    df = (
        df
        .groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema_4)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return None, df,


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_mean(C: np.ndarray) -> np.float64:
    return np.mean(C)


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_max(C: np.ndarray) -> np.float64:
    return np.max(C)


@jit(numba_float64(numba_float64[:]), nopython=True)
def my_numba_var(C: np.ndarray) -> np.float64:
    return np.var(C)


@jit(numba_float64(numba_float64[:]), parallel=True, nopython=True)
def my_looplift_var(E: np.ndarray) -> float:
    n = len(E)
    accE2 = 0.
    for i in prange(n):
        accE2 += E[i] ** 2
    accE = 0.
    for i in prange(n):
        accE += E[i]
    return accE2 / n - (accE / n)**2  # pyright: ignore


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    subgroup_key = dfPartition['subgrp'].iloc[0]
    C = np.array(dfPartition['C'])
    D = np.array(dfPartition['D'])
    posE = np.array(dfPartition[dfPartition.E < 0]['E'])
    return pd.DataFrame([[
        group_key,
        subgroup_key,
        my_numba_mean(C),
        my_numba_max(D),
        my_numba_var(posE),
        my_looplift_var(posE),
    ]], columns=groupby_columns + agg_columns_4)
