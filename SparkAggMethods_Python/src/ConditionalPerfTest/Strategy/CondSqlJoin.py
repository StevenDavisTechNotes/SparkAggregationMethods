from typing import Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame

from SixFieldCommon.SixFieldTestData import DataSet, ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def cond_sql_join(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: DataSet,
) -> Tuple[RDD | None, spark_DataFrame | None]:
    spark = spark_session.spark
    spark.catalog.dropTempView("exampledata")
    data_set.data.dfSrc.createTempView("exampledata")
    df = spark.sql('''
    SELECT
        unconditional.grp, unconditional.subgrp,
        mean_of_C, max_of_D, cond_var_of_E
    FROM
        (SELECT
            grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D
        FROM
            exampledata
        GROUP BY grp , subgrp) unconditional
            LEFT JOIN
        (SELECT
            grp,
                subgrp,
                (
                    cond_sum_of_E_squared  / cond_count_of_E -
                    POWER(cond_sum_of_E / cond_count_of_E, 2)
                ) cond_var_of_E
        FROM
            (SELECT
                grp,
                subgrp,
                cond_sum_of_E_squared,
                cond_sum_of_E,
                cond_count_of_E
        FROM
            (SELECT
                grp,
                subgrp,
                SUM(E * E) AS cond_sum_of_E_squared,
                SUM(E) AS cond_sum_of_E,
                COUNT(*) cond_count_of_E
        FROM
            exampledata
        WHERE
            E < 0
        GROUP BY grp , subgrp) AS Inter1) AS Inter2) conditional
        ON unconditional.grp = conditional.grp
            AND unconditional.subgrp = conditional.subgrp
    ORDER BY grp, subgrp
    ''')
    return (None, df)
