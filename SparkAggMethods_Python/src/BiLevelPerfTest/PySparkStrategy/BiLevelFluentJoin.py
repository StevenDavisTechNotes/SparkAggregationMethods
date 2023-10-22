from typing import Optional, Tuple

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.PySpark_SixFieldTestData import PysparkDataSet
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def bi_fluent_join(
        _spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = data_set.data.dfSrc
    level1 = (
        df
        .groupBy(df.grp)
        .agg(
            func.mean(df.C).alias("mean_of_C"),
            func.max(df.D).alias("max_of_D")
        )
    )
    level2 = (
        df
        .groupBy(df.grp, df.subgrp)
        .agg(
            func.var_pop(df.E).alias("var_of_E"),
            (
                func.sum(df.E * df.E) / func.count(df.E) -
                func.avg(df.E)**2
            ).alias("var_of_E2")
        )
    )
    level3 = (
        level2
        .join(level1, "grp")
        .groupBy(level1.grp)
        .agg(
            func.last(level1.mean_of_C).alias("mean_of_C"),
            func.last(level1.max_of_D).alias("max_of_D"),
            func.avg(level2.var_of_E).alias("avg_var_of_E"),
            func.avg(level2.var_of_E2).alias("avg_var_of_E2")
        )
    )
    level4 = level3.orderBy(level3.grp)
    return None, level4
