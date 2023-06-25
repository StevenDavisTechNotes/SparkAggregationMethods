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


def cond_fluent_join(
    spark_session: TidySparkSession,
    pyData: List[DataPoint],
) -> Tuple[RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    dfData = spark.createDataFrame(pyData)
    uncond = dfData \
        .groupBy(dfData.grp, dfData.subgrp) \
        .agg(
            func.mean(dfData.C).alias("mean_of_C"),
            func.max(dfData.D).alias("max_of_D"))
    cond = dfData \
        .filter(dfData.E < 0) \
        .groupBy(dfData.grp, dfData.subgrp) \
        .agg(
            func.variance(dfData.E).alias("cond_var_of_E"))
    df = uncond \
        .join(cond,
              (uncond.grp == cond.grp) & (uncond.subgrp == cond.subgrp)) \
        .drop(cond.grp) \
        .drop(cond.subgrp) \
        .orderBy(uncond.grp, uncond.subgrp)
    return (None, df)
