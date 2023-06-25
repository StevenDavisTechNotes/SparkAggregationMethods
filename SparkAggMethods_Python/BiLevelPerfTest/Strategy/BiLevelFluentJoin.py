from typing import List, Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
import pyspark.sql.functions as func

from Utils.SparkUtils import TidySparkSession

from ..BiLevelTestData import DataPoint


def bi_fluent_join(
        spark_session: TidySparkSession, pyData: List[DataPoint]
                ) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    spark = spark_session.spark
    # df = spark.createDataFrame(
    #     map(lambda x: astuple(x), pyData), schema=DataPointSchema)
    df = spark.createDataFrame(pyData)
    level1 = df \
        .groupBy(df.grp) \
        .agg(
            func.mean(df.C).alias("mean_of_C"),
            func.max(df.D).alias("max_of_D"))
    level2 = df \
        .groupBy(df.grp, df.subgrp) \
        .agg(
            func.variance(df.E).alias("var_of_E"),
            ((func.sum(df.E * df.E) -
              func.sum(df.E) * func.avg(df.E))
             / (func.count(df.E)-1)).alias("var_of_E2")
        )
    level3 = level2 \
        .join(level1, "grp") \
        .groupBy(level1.grp) \
        .agg(
            func.last(level1.mean_of_C).alias("mean_of_C"),
            func.last(level1.max_of_D).alias("max_of_D"),
            func.avg(level2.var_of_E).alias("avg_var_of_E"),
            func.avg(level2.var_of_E2).alias("avg_var_of_E2")
        ) \
        .orderBy(level1.grp)
    return None, level3