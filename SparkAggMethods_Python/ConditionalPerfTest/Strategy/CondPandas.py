import collections
import gc
import math
import random
import time
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

import numpy
import numpy as np
import pandas as pd
import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
import scipy.stats
from numba import cuda
from numba import float64 as numba_float64
from numba import jit, njit, prange, vectorize
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row, SparkSession
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.window import Window
from ..CondTestData import DataPoint, DataPointSchema

from LinearRegression import linear_regression
from Utils.SparkUtils import TidySparkSession


def cond_pandas(
    spark_session: TidySparkSession,
    pyData: List[DataPoint],
) -> Tuple[RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    groupby_columns = ['grp', 'subgrp']
    agg_columns = ['mean_of_C', 'max_of_D', 'cond_var_of_E', 'cond_var_of_E2']
    df = spark.createDataFrame(pyData)
    postAggSchema = DataTypes.StructType(
        [x for x in DataPointSchema.fields if x.name in groupby_columns] +
        [DataTypes.StructField(name, DataTypes.DoubleType(), False)
         for name in agg_columns])
    #

    def inner_agg_method(dfPartition):
        group_key = dfPartition['grp'].iloc[0]
        subgroup_key = dfPartition['subgrp'].iloc[0]
        C = dfPartition['C']
        D = dfPartition['D']
        posE = dfPartition[dfPartition.E < 0]['E']
        return pd.DataFrame([[
            group_key,
            subgroup_key,
            C.mean(),
            D.max(),
            posE.var(),
            posE
            .agg(lambda E:
                 ((E * E).sum() -
                     E.sum()**2/E.count())/(E.count()-1))
            .mean(),
        ]], columns=groupby_columns + agg_columns)
    #
    aggregates = df \
        .groupby(df.grp, df.subgrp).applyInPandas(inner_agg_method, postAggSchema) \
        .orderBy('grp', 'subgrp')
    return None, aggregates
