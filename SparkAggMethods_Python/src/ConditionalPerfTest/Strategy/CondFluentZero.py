from typing import Tuple

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession


def cond_fluent_zero(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet,
) -> Tuple[RDD | None, spark_DataFrame | None]:
    dfData = data_set.data.dfSrc
    dfInter = (
        dfData
        .groupBy(dfData.grp, dfData.subgrp)
        .agg(func.mean(dfData.C).alias("mean_of_C"),
             func.max(dfData.D).alias("max_of_D"),
             func.sum(func.when(dfData.E < 0, dfData.E * dfData.E)
                      .otherwise(0)).alias("cond_sum_of_E_squared"),
             func.sum(func.when(dfData.E < 0, dfData.E)
                      .otherwise(0)).alias("cond_sum_of_E"),
             func.sum(func.when(dfData.E < 0, 1)
                      .otherwise(0)).alias("cond_count"))
    )
    dfInter = dfInter\
        .withColumn("cond_var_of_E",
                    func.when(dfInter.cond_count > 0,
                              dfInter.cond_sum_of_E_squared / dfInter.cond_count
                              - (dfInter.cond_sum_of_E / dfInter.cond_count)**2))
    dfInter = dfInter\
        .select('grp', 'subgrp', 'mean_of_C', 'max_of_D', 'cond_var_of_E')
    dfInter = dfInter\
        .orderBy(dfInter.grp, dfInter.subgrp)
    return None, dfInter
