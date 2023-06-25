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


def cond_rdd_grpmap(
    spark_session: TidySparkSession,
    pyData: List[DataPoint],
) -> Tuple[RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    sc = spark_session.spark_context

    def processData1(key, iterator):
        import math
        sum_of_C = 0
        unconditional_count = 0
        max_of_D = None
        cond_sum_of_E_squared = 0
        cond_sum_of_E = 0
        cond_count_of_E = 0
        for item in iterator:
            sum_of_C = sum_of_C + item.C
            unconditional_count = unconditional_count + 1
            max_of_D = item.D \
                if max_of_D is None or max_of_D < item.D \
                else max_of_D
            if item.E < 0:
                cond_sum_of_E_squared = \
                    cond_sum_of_E_squared + item.E * item.E
                cond_sum_of_E = cond_sum_of_E + item.E
                cond_count_of_E = cond_count_of_E + 1
        mean_of_C = sum_of_C / unconditional_count \
            if unconditional_count > 0 else math.nan
        cond_var_of_E = math.nan
        if cond_count_of_E >= 2:
            cond_var_of_E = \
                (
                    cond_sum_of_E_squared
                    - cond_sum_of_E * cond_sum_of_E / cond_count_of_E
                ) / (cond_count_of_E - 1)
        return (key,
                Row(grp=key[0],
                    subgrp=key[1],
                    mean_of_C=mean_of_C,
                    max_of_D=max_of_D,
                    cond_var_of_E=cond_var_of_E))

    rddData = sc.parallelize(pyData)
    rddResult = (
        cast_no_arg_sort_by_key(
            rddData
            .groupBy(lambda x: (x.grp, x.subgrp))
            .map(lambda pair: processData1(pair[0], pair[1]))
            .repartition(1)
        )
        .sortByKey().values()
    )
    df = spark.createDataFrame(rddResult)\
        .select('grp', 'subgrp', 'mean_of_C', 'max_of_D', 'cond_var_of_E')
    return None, df
