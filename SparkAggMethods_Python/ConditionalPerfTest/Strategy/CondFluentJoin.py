from typing import Tuple

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession


def cond_fluent_join(
    spark_session: TidySparkSession,
    exec_params: ExecutionParameters,
    data_set: DataSet,
) -> Tuple[RDD | None, spark_DataFrame | None]:
    dfData = data_set.dfSrc
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
