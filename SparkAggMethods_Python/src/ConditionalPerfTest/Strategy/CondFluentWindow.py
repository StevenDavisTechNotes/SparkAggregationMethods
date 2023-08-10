from typing import Tuple

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql.window import Window

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def cond_fluent_window(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: DataSet,
) -> Tuple[RDD | None, spark_DataFrame | None]:
    dfData = data_set.data.dfSrc
    dfData = dfData \
        .withColumn("cond", func.when(dfData.E < 0, -1).otherwise(+1))
    dfData = dfData \
        .orderBy(dfData.grp, dfData.subgrp, dfData.cond, dfData.id)
    window = (
        Window
        .partitionBy(dfData.grp, dfData.subgrp, dfData.cond)
        .orderBy(dfData.id)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    dfData = dfData \
        .withColumn("cond_var_of_E_2_pre1",
                    func.when(dfData.cond < 0,
                              func.var_pop(dfData.E)
                              .over(window))
                    )
    dfData = (
        dfData
        .groupBy(dfData.grp, dfData.subgrp, dfData.cond)
        .agg(
            func.sum(dfData.C).alias("sum_of_C_pre"),
            func.count(dfData.C).alias("count_of_C_pre"),
            func.max(dfData.D).alias("max_of_D_pre"),
            func.var_pop(func.when(dfData.E < 0, dfData.E)
                         ).alias("cond_var_of_E_1_pre"),
            func.last(dfData.cond_var_of_E_2_pre1).alias("cond_var_of_E_2_pre2"))
    )

    dfData = (
        dfData
        .groupBy(dfData.grp, dfData.subgrp)
        .agg(
            (func.sum(dfData.sum_of_C_pre)
             / func.sum(dfData.count_of_C_pre)
             ).alias("mean_of_C"),
            func.max(dfData.max_of_D_pre).alias("max_of_D"),
            func.max(dfData.cond_var_of_E_1_pre).alias("cond_var_of_E_1"),
            func.max(dfData.cond_var_of_E_2_pre2).alias("cond_var_of_E_2")
        )
    )
    dfData = dfData.orderBy(dfData.grp, dfData.subgrp)
    return None, dfData
