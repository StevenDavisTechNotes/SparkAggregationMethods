
from six_field_test_data.six_generate_test_data import (
    DataSetPyspark, TChallengePendingAnswerPythonPyspark)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.tidy_spark_session import TidySparkSession


def cond_pyspark_sql_join(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark,
) -> TChallengePendingAnswerPythonPyspark:
    spark = spark_session.spark
    spark.catalog.dropTempView("example_data")
    data_set.data.dfSrc.createTempView("example_data")
    df = spark.sql('''
    SELECT
        unconditional.grp, unconditional.subgrp,
        mean_of_C, max_of_D, cond_var_of_E
    FROM
        (SELECT
            grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D
        FROM
            example_data
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
            example_data
        WHERE
            E < 0
        GROUP BY grp , subgrp) AS Inter1) AS Inter2) conditional
        ON unconditional.grp = conditional.grp
            AND unconditional.subgrp = conditional.subgrp
    ORDER BY grp, subgrp
    ''')
    return df
