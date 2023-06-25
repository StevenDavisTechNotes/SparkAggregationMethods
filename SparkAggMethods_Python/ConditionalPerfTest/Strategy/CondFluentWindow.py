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


def cond_fluent_window(
    spark_session: TidySparkSession,
    pyData: List[DataPoint],
) -> Tuple[RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    dfData = spark.createDataFrame(pyData)
    dfData = dfData \
        .withColumn("cond", func.when(dfData.E < 0, -1).otherwise(+1))
    dfData = dfData \
        .orderBy(dfData.grp, dfData.subgrp, dfData.cond, dfData.id)
    window = Window \
        .partitionBy(dfData.grp, dfData.subgrp, dfData.cond) \
        .orderBy(dfData.id)\
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    dfData = dfData \
        .withColumn("cond_var_of_E_2_pre1",
                    func.when(dfData.cond < 0,
                              func.variance(dfData.E)
                              .over(window)))
    dfData = dfData \
        .groupBy(dfData.grp, dfData.subgrp, dfData.cond)\
        .agg(func.sum(dfData.C).alias("sum_of_C_pre"),
             func.count(dfData.C).alias("count_of_C_pre"),
             func.max(dfData.D).alias("max_of_D_pre"),
             func.variance(func.when(dfData.E < 0, dfData.E)
                           ).alias("cond_var_of_E_1_pre"),
             func.last(dfData.cond_var_of_E_2_pre1).alias("cond_var_of_E_2_pre2"))

    dfData = dfData \
        .groupBy(dfData.grp, dfData.subgrp)\
        .agg((func.sum(dfData.sum_of_C_pre)
              / func.sum(dfData.count_of_C_pre)
              ).alias("mean_of_C"),
             func.max(dfData.max_of_D_pre).alias("max_of_D"),
             func.max(dfData.cond_var_of_E_1_pre).alias("cond_var_of_E_1"),
             func.max(dfData.cond_var_of_E_2_pre2).alias("cond_var_of_E_2"))\
        .orderBy(dfData.grp, dfData.subgrp)
    return None, dfData
