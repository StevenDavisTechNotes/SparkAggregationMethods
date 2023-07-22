from typing import Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters

from Utils.SparkUtils import TidySparkSession


def vanilla_sql(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    df = data_set.dfSrc
    spark_session.spark.catalog.dropTempView("exampledata")
    df.createTempView("exampledata")
    df = spark_session.spark.sql('''
    SELECT
        grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D,
        VAR_POP(E) var_of_E,
        (
            SUM(E * E) / COUNT(E) -
            POWER(SUM(E) / COUNT(E), 2)
        ) var_of_E2
    FROM
        exampledata
    GROUP BY grp, subgrp
    ORDER BY grp, subgrp
    ''')
    return None, df
