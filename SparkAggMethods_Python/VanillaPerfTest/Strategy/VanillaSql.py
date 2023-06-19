from typing import List, Tuple, Optional

from dataclasses import astuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..VanillaTestData import DataPoint, DataPointSchema


def vanilla_sql(
    spark_session: TidySparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    spark = spark_session.spark
    df = spark.createDataFrame(
        map(lambda x: astuple(x), pyData), schema=DataPointSchema)
    spark.catalog.dropTempView("exampledata")
    df.createTempView("exampledata")
    df = spark.sql('''
    SELECT 
        grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D, 
        VAR_SAMP(E) var_of_E,
        (
            SUM(E*E) - 
            SUM(E) * SUM(E) / COUNT(E)
        ) / (COUNT(E) - 1) var_of_E2
    FROM
        exampledata
    GROUP BY grp, subgrp
    ORDER BY grp, subgrp
    ''')
    return None, df
