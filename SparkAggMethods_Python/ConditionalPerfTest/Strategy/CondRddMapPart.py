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
from Utils.SparkUtils import TidySparkSession, cast_no_arg_sort_by_key

SubTotal = collections.namedtuple("SubTotal",
                                  ["running_sum_of_C", "running_uncond_count", "running_max_of_D",
                                   "running_cond_sum_of_E_squared", "running_cond_sum_of_E",
                                   "running_cond_count"])


@dataclass(frozen=True)
class GrpTotal:
    grp: int
    subgrp: int
    mean_of_C: float
    max_of_D: float
    cond_var_of_E: float


def cond_rdd_mappart(
    spark_session: TidySparkSession,
    pyData: List[DataPoint],
) -> Tuple[RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    sc = spark_session.spark_context
    rddData = sc.parallelize(pyData)

    class MutableRunningTotal:
        def __init__(self):
            self.running_sum_of_C = 0
            self.running_uncond_count = 0
            self.running_max_of_D = None
            self.running_cond_sum_of_E_squared = 0
            self.running_cond_sum_of_E = 0
            self.running_cond_count = 0

    def partitionTriage(splitIndex, iterator):
        running_subtotals = {}
        for v in iterator:
            k = (v.grp, v.subgrp)
            if k not in running_subtotals:
                running_subtotals[k] = MutableRunningTotal()
            sub = running_subtotals[k]
            sub.running_sum_of_C += v.C
            sub.running_uncond_count += 1
            sub.running_max_of_D = \
                sub.running_max_of_D \
                if sub.running_max_of_D is not None and \
                sub.running_max_of_D > v.D \
                else v.D
            if v.E < 0:
                sub.running_cond_sum_of_E_squared += v.E * v.E
                sub.running_cond_sum_of_E += v.E
                sub.running_cond_count += 1
        for k in running_subtotals:
            sub = running_subtotals[k]
            yield (k, SubTotal(
                running_sum_of_C=sub.running_sum_of_C,
                running_uncond_count=sub.running_uncond_count,
                running_max_of_D=sub.running_max_of_D,
                running_cond_sum_of_E_squared=sub.running_cond_sum_of_E_squared,
                running_cond_sum_of_E=sub.running_cond_sum_of_E,
                running_cond_count=sub.running_cond_count))

    def mergeCombiners3(key, iterable):
        lsub = MutableRunningTotal()
        for rsub in iterable:
            lsub.running_sum_of_C += rsub.running_sum_of_C
            lsub.running_uncond_count += rsub.running_uncond_count
            lsub.running_max_of_D = lsub.running_max_of_D \
                if lsub.running_max_of_D is not None and \
                lsub.running_max_of_D > rsub.running_max_of_D \
                else rsub.running_max_of_D
            lsub.running_cond_sum_of_E_squared += \
                rsub.running_cond_sum_of_E_squared
            lsub.running_cond_sum_of_E += rsub.running_cond_sum_of_E
            lsub.running_cond_count += rsub.running_cond_count
        return SubTotal(
            running_sum_of_C=lsub.running_sum_of_C,
            running_uncond_count=lsub.running_uncond_count,
            running_max_of_D=lsub.running_max_of_D,
            running_cond_sum_of_E_squared=lsub.running_cond_sum_of_E_squared,
            running_cond_sum_of_E=lsub.running_cond_sum_of_E,
            running_cond_count=lsub.running_cond_count)

    def finalAnalytics2(key, total):
        sum_of_C = total.running_sum_of_C
        uncond_count = total.running_uncond_count
        max_of_D = total.running_max_of_D
        cond_sum_of_E_squared = total.running_cond_sum_of_E_squared
        cond_sum_of_E = total.running_cond_sum_of_E
        cond_count = total.running_cond_count
        return Row(
            grp=key[0], subgrp=key[1],
            mean_of_C=math.nan
            if cond_count < 1 else
            sum_of_C / uncond_count,
            max_of_D=max_of_D,
            cond_var_of_E=math.nan
            if cond_count < 2 else
            (
                cond_sum_of_E_squared -
                cond_sum_of_E *
                cond_sum_of_E / cond_count
            ) / (cond_count - 1))

    rddSumCount = (
        rddData
        .mapPartitionsWithIndex(partitionTriage)
        .groupByKey()
        .map(lambda kv: (kv[0], mergeCombiners3(kv[0], kv[1])))
        .map(lambda kv: (kv[0], finalAnalytics2(kv[0], kv[1])))
    )
    rddSumCount = (
        cast_no_arg_sort_by_key(rddSumCount)
        .sortByKey()
    )
    return rddSumCount, None
