from typing import Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession


def bi_sql_join(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    dfSrc = data_set.dfSrc
    spark = spark_session.spark

    spark.catalog.dropTempView("exampledata")
    dfSrc.createTempView("exampledata")
    df = spark.sql('''
    SELECT
        level1.grp,
        LAST(level1.mean_of_C) mean_of_C,
        LAST(level1.max_of_D) max_of_D,
        AVG(level2.var_of_E) avg_var_of_E,
        AVG(level2.var_of_E2) avg_var_of_E2
    FROM
        (SELECT
            grp, AVG(C) mean_of_C, MAX(D) max_of_D
        FROM
            exampledata
        GROUP BY grp) AS level1
            LEFT JOIN
        (SELECT
                grp,
                subgrp,
                VAR_POP(E) var_of_E,
                (SUM(E * E) /COUNT(E) -
                POWER(AVG(E), 2)) var_of_E2
            FROM
                exampledata
            GROUP BY grp , subgrp
        ) AS level2
            ON level1.grp = level2.grp
    GROUP BY level1.grp
    ORDER BY level1.grp
    ''')
    return None, df
