from typing import Optional, Tuple

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql.window import Window

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def bi_fluent_window(
        _spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = data_set.data.dfSrc
    window = Window \
        .partitionBy(df.grp, df.subgrp) \
        .orderBy(df.id)
    df = (
        df
        .orderBy(df.grp, df.subgrp, df.id)
        .withColumn("sub_var_of_E",
                    func.var_pop(df.E)
                    .over(window))
    )
    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .agg(func.sum(df.C).alias("sub_sum_of_C"),
             func.count(df.C).alias("sub_count"),
             func.max(df.D).alias("sub_max_of_D"),
             func.last(df.sub_var_of_E).alias("sub_var_of_E1"),
             func.var_pop(df.E).alias("sub_var_of_E2"))
    )
    df = (
        df
        .groupBy(df.grp)
        .agg(
            (func.sum(df.sub_sum_of_C) /
             func.sum(df.sub_count)).alias("mean_of_C"),
            func.max(df.sub_max_of_D).alias("max_of_D"),
            func.avg(df.sub_var_of_E1).alias("avg_var_of_E1"),
            func.avg(df.sub_var_of_E2).alias("avg_var_of_E2"))
    )
    df = df.orderBy(df.grp)
    return None, df
