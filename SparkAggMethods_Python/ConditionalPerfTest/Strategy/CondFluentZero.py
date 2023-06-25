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
from ..CondTestData import DataPoint

from LinearRegression import linear_regression
from Utils.SparkUtils import TidySparkSession


def cond_fluent_zero(
    spark_session: TidySparkSession,
    pyData: List[DataPoint],
) -> Tuple[RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    dfData = spark.createDataFrame(pyData)
    dfInter = dfData \
        .groupBy(dfData.grp, dfData.subgrp)\
        .agg(func.mean(dfData.C).alias("mean_of_C"),
             func.max(dfData.D).alias("max_of_D"),
             func.sum(func.when(dfData.E < 0, dfData.E * dfData.E)
                      .otherwise(0)).alias("cond_sum_of_E_squared"),
             func.sum(func.when(dfData.E < 0, dfData.E)
                      .otherwise(0)).alias("cond_sum_of_E"),
             func.sum(func.when(dfData.E < 0, 1)
                      .otherwise(0)).alias("cond_count"))
    dfInter = dfInter\
        .withColumn("cond_var_of_E",
                    func.when(dfInter.cond_count > 0,
                              (
                                  dfInter.cond_sum_of_E_squared
                                  - dfInter.cond_sum_of_E * dfInter.cond_sum_of_E
                                  / dfInter.cond_count
                              ) / (dfInter.cond_count-1)))
    dfInter = dfInter\
        .select('grp', 'subgrp', 'mean_of_C', 'max_of_D', 'cond_var_of_E')\
        .orderBy(dfData.grp, dfData.subgrp)
    return None, dfInter
