from typing import List, Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from Utils.SparkUtils import TidySparkSession

from ..BiLevelTestData import DataPoint


def bi_sql_nested(
        spark_session: TidySparkSession, pyData: List[DataPoint]
                ) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    spark = spark_session.spark
    # df = spark.createDataFrame(
    #     map(lambda x: astuple(x), pyData), schema=DataPointSchema)
    dfData = spark.createDataFrame(pyData)
    spark.catalog.dropTempView("exampledata")
    dfData.createTempView("exampledata")
    df=spark.sql('''
    SELECT 
            grp,
            SUM(sub_sum_of_C) / SUM(sub_count) as mean_of_C,
            MAX(sub_max_of_D) as max_of_D,
            AVG(sub_var_of_E) as avg_var_of_E,
            AVG(
                (
                    sub_sum_of_E_squared - 
                    sub_sum_of_E * sub_sum_of_E / sub_count
                ) / (sub_count - 1)
               ) as avg_var_of_E2
    FROM
        (SELECT 
                grp, subgrp, 
                count(C) as sub_count, 
                sum(C) as sub_sum_of_C, 
                max(D) as sub_max_of_D, 
                variance(E) as sub_var_of_E,
                sum(E * E) as sub_sum_of_E_squared, 
                sum(E) as sub_sum_of_E
            FROM
                exampledata
            GROUP BY grp, subgrp) level2
    GROUP BY grp
    ORDER BY grp
    ''')
    return None, df

