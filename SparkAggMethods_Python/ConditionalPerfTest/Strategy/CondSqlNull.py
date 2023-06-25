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


def cond_sql_null(
    spark_session: TidySparkSession,
    pyData: List[DataPoint],
) -> Tuple[RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    dfData = spark.createDataFrame(pyData)
    spark.catalog.dropTempView("exampledata")
    dfData.createTempView("exampledata")
    df = spark.sql('''
    SELECT 
        grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D,
        VARIANCE(CASE WHEN E < 0 THEN E ELSE NULL END) AS cond_var_of_E
    FROM exampledata
    GROUP BY grp, subgrp
    ORDER BY grp, subgrp
    ''')
    return (None, df)
