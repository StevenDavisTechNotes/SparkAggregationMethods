from typing import List, Optional, Tuple

import pyspark.sql.functions as func
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql.window import Window

from Utils.SparkUtils import TidySparkSession

from ..BiLevelTestData import DataPoint


def bi_fluent_window(
    spark_session: TidySparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    spark = spark_session.spark
    # df = spark.createDataFrame(
    #     map(lambda x: astuple(x), pyData), schema=DataPointSchema)
    df = spark.createDataFrame(pyData)
    window = Window \
        .partitionBy(df.grp, df.subgrp) \
        .orderBy(df.id)
    df = df \
        .orderBy(df.grp, df.subgrp, df.id)\
        .withColumn("sub_var_of_E",
                    func.variance(df.E)
                    .over(window))
    df = df \
        .groupBy(df.grp, df.subgrp)\
        .agg(func.sum(df.C).alias("sub_sum_of_C"),
             func.count(df.C).alias("sub_count"),
             func.max(df.D).alias("sub_max_of_D"),
             func.last(df.sub_var_of_E).alias("sub_var_of_E1"),
             func.variance(df.E).alias("sub_var_of_E2"))
    df = df \
        .groupBy(df.grp)\
        .agg(
            (func.sum(df.sub_sum_of_C) /
             func.sum(df.sub_count)).alias("mean_of_C"),
            func.max(df.sub_max_of_D).alias("max_of_D"),
            func.avg(df.sub_var_of_E1).alias("avg_var_of_E1"),
            func.avg(df.sub_var_of_E2).alias("avg_var_of_E2"))\
        .orderBy(df.grp)
    return None, df