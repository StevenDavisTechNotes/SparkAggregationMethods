from typing import Optional, Tuple

import numpy as np
import pandas as pd
from numba import float64 as numba_float64
from numba import jit, prange
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession

from ..BiLevelDataTypes import agg_columns, groupby_columns, postAggSchema


def bi_pandas_numba(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = data_set.dfSrc

    @jit(numba_float64(numba_float64[:]), nopython=True)
    def my_numba_mean(C):
        return np.mean(C)

    @jit(numba_float64(numba_float64[:]), nopython=True)
    def my_numba_max(C):
        return np.max(C)

    @jit(numba_float64(numba_float64[:]), nopython=True)
    def my_numba_var(C):
        return np.var(C)

    @jit(numba_float64(numba_float64[:]), parallel=True, nopython=True)
    def my_looplift_var(E):
        n = len(E)
        accE2 = 0.
        for i in prange(n):
            accE2 += E[i] ** 2
        accE = 0.
        for i in prange(n):
            accE += E[i]
        return (accE2 - accE**2/n)/(n-1)

    def inner_agg_method(dfPartition):
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
        ]], columns=groupby_columns + agg_columns)

    aggregates = df.groupby(df.grp).applyInPandas(inner_agg_method, postAggSchema)
    return None, aggregates
