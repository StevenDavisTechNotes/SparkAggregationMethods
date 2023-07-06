from typing import Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession


def bi_sql_nested(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:
    dfSrc = data_set.dfSrc
    spark = spark_session.spark

    spark.catalog.dropTempView("exampledata")
    dfSrc.createTempView("exampledata")
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

