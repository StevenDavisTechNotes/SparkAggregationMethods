from typing import Optional, Tuple

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession


def bi_fluent_nested(
    _spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = data_set.dfSrc
    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .agg(
            func.mean(df.C).alias("sub_mean_of_C"),
            func.count(df.C).alias("sub_count"),
            func.sum(df.C).alias("sub_sum_of_C"),
            func.max(df.D).alias("sub_max_of_D"),
            func.var_pop(df.E).alias("sub_var_of_E"),
            func.sum(df.E * df.E).alias("sub_sum_of_E_squared"),
            func.sum(df.E).alias("sub_sum_of_E")
        )
    )
    df = (
        df
        .groupBy(df.grp)
        .agg(
            (
                func.sum(df.sub_mean_of_C * df.sub_count)
                / func.sum(df.sub_count)
            ).alias("mean_of_C"),
            func.max(df.sub_max_of_D).alias("max_of_D"),
            func.avg(df.sub_var_of_E).alias("avg_var_of_E"),
            func.avg(
                df.sub_sum_of_E_squared / df.sub_count
                - (df.sub_sum_of_E / df.sub_count)**2
            ).alias("avg_var_of_E2")
        )
    )
    df = (
        df
        .select('grp', 'mean_of_C', 'max_of_D',
                'avg_var_of_E', 'avg_var_of_E2')
        .orderBy(df.grp)
    )
    return None, df
