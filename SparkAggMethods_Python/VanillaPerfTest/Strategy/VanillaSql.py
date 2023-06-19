from typing import List, Tuple, Optional

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame as spark_DataFrame

from ..VanillaTestData import DataPointAsTuple, DataPointSchema


def vanilla_sql(
        spark: SparkSession, pyData: List[DataPointAsTuple]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = spark.createDataFrame(pyData,        schema=DataPointSchema)
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
