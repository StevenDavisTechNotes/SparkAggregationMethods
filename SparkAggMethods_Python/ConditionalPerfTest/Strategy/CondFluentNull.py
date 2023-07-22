from typing import Tuple

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession


def cond_fluent_null(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet,
) -> Tuple[RDD | None, spark_DataFrame | None]:
    dfData = data_set.dfSrc
    dfInter = (
        dfData
        .groupBy(dfData.grp, dfData.subgrp)
        .agg(func.mean(dfData.C).alias("mean_of_C"),
             func.max(dfData.D).alias("max_of_D"),
             func.var_pop(func.when(dfData.E < 0, dfData.E))
             .alias("cond_var_of_E"))
    )
    df = dfInter.select('grp', 'subgrp', 'mean_of_C', 'max_of_D', 'cond_var_of_E')
    df = df.orderBy(df.grp, df.subgrp)
    return (None, df)
