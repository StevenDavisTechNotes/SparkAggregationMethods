from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import pyspark.sql.types as DataTypes
from numba import float64 as numba_float64
from numba import jit, prange
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql.pandas.functions import PandasUDFType, pandas_udf

from Utils.SparkUtils import TidySparkSession

from ..BiLevelTestData import DataPoint, DataPointSchema


def bi_pandas_numba(
    spark_session: TidySparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    spark = spark_session.spark
    # df = spark.createDataFrame(
    #     map(lambda x: astuple(x), pyData), schema=DataPointSchema)
    groupby_columns = ['grp']
    agg_columns = ['mean_of_C', 'max_of_D', 'avg_var_of_E', 'avg_var_of_E2']
    df = spark.createDataFrame(pyData)
    postAggSchema = DataTypes.StructType(
        [x for x in DataPointSchema.fields if x.name in groupby_columns] +
        [DataTypes.StructField(name, DataTypes.DoubleType(), False) for name in agg_columns])

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

    @pandas_udf(postAggSchema, PandasUDFType.GROUPED_MAP)
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

    aggregates = df.groupby(df.grp).apply(inner_agg_method)
    return None, aggregates
