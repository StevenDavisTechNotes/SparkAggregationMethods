from typing import Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.SparkUtils import TidySparkSession


def cond_sql_nested(
    spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet,
) -> Tuple[RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    spark.catalog.dropTempView("exampledata")
    data_set.data.dfSrc.createTempView("exampledata")
    df = spark.sql('''
    SELECT
            grp, subgrp,
            sum_of_C / count as mean_of_C,
            max_of_D,
            (
                cond_sum_of_E_squared  / cond_count_of_E -
                POWER(cond_sum_of_E / cond_count_of_E, 2)
            ) cond_var_of_E
    FROM
        (SELECT
                grp, subgrp,
                sum(sub_count) count,
                sum(sub_sum_of_C) as sum_of_C,
                max(sub_max_of_D) as max_of_D,
                sum(CASE e_cond WHEN TRUE THEN sub_sum_of_E_squared ELSE 0 END) as cond_sum_of_E_squared,
                sum(CASE e_cond WHEN TRUE THEN sub_sum_of_E ELSE 0 END) as cond_sum_of_E,
                sum(CASE e_cond WHEN TRUE THEN sub_count ELSE 0 END) as cond_count_of_E
        FROM
            (SELECT
                    grp, subgrp,
                    E<0 e_cond,
                    count(C) as sub_count,
                    sum(C) as sub_sum_of_C,
                    max(D) as sub_max_of_D,
                    sum(E * E) as sub_sum_of_E_squared,
                    sum(E) as sub_sum_of_E
                FROM
                    exampledata
                GROUP BY grp, subgrp, e<0) sub1
        GROUP BY grp, subgrp) sub2
    ORDER BY grp, subgrp
    ''')
    return None, df
