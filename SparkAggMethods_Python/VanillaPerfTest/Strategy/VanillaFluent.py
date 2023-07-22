from typing import Optional, Tuple

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters

from Utils.SparkUtils import TidySparkSession


def vanilla_fluent(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = data_set.dfSrc
    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .agg(
            func.mean(df.C).alias("mean_of_C"),
            func.max(df.D).alias("max_of_D"),
            func.var_pop(df.E).alias("var_of_E"),
            (
                func.sum(df.E * df.E) / func.count(df.E)
                - func.pow(func.sum(df.E) / func.count(df.E), 2)
            ).alias("var_of_E2")
        )
        .orderBy(df.grp, df.subgrp))
    return None, df
