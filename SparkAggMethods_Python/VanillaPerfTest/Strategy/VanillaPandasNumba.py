from typing import List, Tuple, Optional

from dataclasses import astuple

import pandas as pd
from numba import jit, prange
from numba import float64 as numba_float64
import numpy

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..VanillaTestData import DataPoint, DataPointSchema, groupby_columns, agg_columns, postAggSchema


def vanilla_pandas_numba(
    spark_session: TidySparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:

    @jit(numba_float64(numba_float64[:]), nopython=True)
    def my_numba_mean(C):
        return numpy.mean(C)

    @jit(numba_float64(numba_float64[:]), nopython=True)
    def my_numba_max(C):
        return numpy.max(C)

    @jit(numba_float64(numba_float64[:]), nopython=True)
    def my_numba_var(C):
        return numpy.var(C)

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

    df = spark_session.spark.createDataFrame(
        map(lambda x: astuple(x), pyData), schema=DataPointSchema)
    aggregates = (
        df.groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    return None, aggregates
