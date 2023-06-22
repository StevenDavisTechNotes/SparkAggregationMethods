from typing import List, Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..BiLevelTestData import DataPoint


def bi_sql_join(
    spark_session: TidySparkSession, pyData: List[DataPoint]
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    spark = spark_session.spark
    # df = spark.createDataFrame(
    #     map(lambda x: astuple(x), pyData), schema=DataPointSchema)
    df = spark.createDataFrame(pyData)
    spark.catalog.dropTempView("exampledata")
    df.createTempView("exampledata")
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
                VARIANCE(E) var_of_E,
                (SUM(E * E) - 
                SUM(E)*AVG(E))/(COUNT(E)-1) var_of_E2
            FROM
                exampledata
            GROUP BY grp , subgrp
        ) AS level2    
            ON level1.grp = level2.grp
    GROUP BY level1.grp
    ORDER BY level1.grp
    ''')
    return None, df
