from typing import Tuple

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession


def cond_fluent_nested(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet,
) -> Tuple[RDD | None, spark_DataFrame | None]:
    dfData = data_set.dfSrc
    dfInter = dfData\
        .withColumn('cond', func.when(dfData.E < 0, -1).otherwise(1))
    dfInter = (
        dfInter
        .groupBy(dfInter.grp, dfInter.subgrp, dfInter.cond)
        .agg(func.mean(dfData.C).alias("sub_mean_of_C"),
             func.count(dfData.C).alias("sub_count"),
             func.sum(dfData.C).alias("sub_sum_of_C"),
             func.max(dfData.D).alias("sub_max_of_D"),
             func.var_pop(dfData.E).alias("sub_var_of_E"),
             func.sum(dfData.E * dfData.E).alias("sub_sum_of_E_squared"),
             func.sum(dfData.E).alias("sub_sum_of_E")))
    dfInter = (
        dfInter
        .groupBy(dfInter.grp, dfInter.subgrp)
        .agg(func.mean(dfInter.sub_mean_of_C).alias("wrong_mean_of_C"),
             (
            func.sum(dfInter.sub_mean_of_C * dfInter.sub_count)
            / func.sum(dfInter.sub_count)
        ).alias("mean_of_C2"),
            func.sum(dfInter.sub_count).alias("uncond_count"),
            func.sum(func.when(dfInter.cond < 0, dfInter.sub_count)
                     .otherwise(0)).alias("cond_count"),
            func.sum(dfInter.sub_sum_of_C).alias("sum_of_C"),
            func.max(dfInter.sub_max_of_D).alias("max_of_D"),
            func.sum(func.when(dfInter.cond < 0, dfInter.sub_var_of_E)
                     .otherwise(0)).alias("cond_var_of_E"))
    )
    dfInter = dfInter \
        .withColumn('mean_of_C', dfInter.sum_of_C / dfInter.uncond_count)
    dfResult = dfInter.select('grp', 'subgrp', 'mean_of_C', 'mean_of_C2', 'wrong_mean_of_C',
                              'max_of_D', 'cond_var_of_E')
    dfResult = dfResult.orderBy(dfResult.grp, dfResult.subgrp)
    return None, dfResult
