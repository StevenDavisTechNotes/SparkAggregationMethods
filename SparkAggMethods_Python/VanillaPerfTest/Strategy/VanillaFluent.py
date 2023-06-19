from typing import List

from pyspark.sql import SparkSession
import pyspark.sql.functions as func

from ..VanillaTestData import DataPointAsTuple, DataPointSchema


def vanilla_fluent(spark: SparkSession, pyData: List[DataPointAsTuple]):
    df = spark.createDataFrame(pyData, schema=DataPointSchema)
    df = df \
        .groupBy(df.grp, df.subgrp) \
        .agg(
            func.mean(df.C).alias("mean_of_C"),
            func.max(df.D).alias("max_of_D"),
            func.variance(df.E).alias("var_of_E"),
            ((
                func.sum(df.E * df.E)
                - func.pow(func.sum(df.E), 2)/func.count(df.E)
            )/(func.count(df.E)-1)).alias("var_of_E2")
        )\
        .orderBy(df.grp, df.subgrp)
    return None, df
