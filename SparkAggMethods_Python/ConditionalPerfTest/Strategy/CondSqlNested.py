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


def cond_sql_nested(
    spark_session: TidySparkSession,
    pyData: List[DataPoint],
) -> Tuple[RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    dfData = spark.createDataFrame(pyData)
    spark.catalog.dropTempView("exampledata")
    dfData.createTempView("exampledata")
    df = spark.sql('''
    SELECT
            grp, subgrp,
            sum_of_C / count as mean_of_C,
            max_of_D,
            (
                cond_sum_of_E_squared -
                cond_sum_of_E * cond_sum_of_E / cond_count_of_E
            ) / (cond_count_of_E - 1) cond_var_of_E
    FROM
        (SELECT
                grp, subgrp,
                sum(sub_count) count,
                sum(sub_sum_of_C) as sum_of_C,
                max(sub_max_of_D) as max_of_D,
                sum(CASE e_cond WHEN TRUE THEN sub_sum_of_E_squared ELSE 0 END) as cond_sum_of_E_squared,
                sum(CASE e_cond WHEN TRUE THEN sub_sum_of_E ELSE 0 END) as cond_sum_of_E,
                sum(CASE e_cond WHEN TRUE THEN sub_count ELSE 0 END) as cond_count_of_E
        FROM
            (SELECT
                    grp, subgrp,
                    E<0 e_cond,
                    count(C) as sub_count,
                    sum(C) as sub_sum_of_C,
                    max(D) as sub_max_of_D,
                    sum(E * E) as sub_sum_of_E_squared,
                    sum(E) as sub_sum_of_E
                FROM
                    exampledata
                GROUP BY grp, subgrp, e<0) sub1
        GROUP BY grp, subgrp) sub2
    ORDER BY grp, subgrp
    ''')
    return None, df
