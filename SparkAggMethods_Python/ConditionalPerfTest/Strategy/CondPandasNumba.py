from typing import Tuple

import numpy as np
import pandas as pd
from numba import float64 as numba_float64
from numba import jit, prange
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession

from ..CondDataTypes import agg_columns_4, groupby_columns, postAggSchema_4


def cond_pandas_numba(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet,
) -> Tuple[RDD | None, spark_DataFrame | None]:
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
        return accE2 / n - (accE / n)**2

    def inner_agg_method(dfPartition):
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

    df = (
        df
        .groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema_4)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return None, df,
