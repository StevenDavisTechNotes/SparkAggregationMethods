from src.six_field_test_data.six_generate_test_data import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.six_field_test_data.six_test_data_types import SixTestExecutionParameters
from src.utils.tidy_spark_session import TidySparkSession


def bi_level_pyspark_sql_join(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    dfSrc = data_set.data.df_src
    spark = spark_session.spark

    spark.catalog.dropTempView("example_data")
    dfSrc.createTempView("example_data")
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
            example_data
        GROUP BY grp) AS level1
            LEFT JOIN
        (SELECT
                grp,
                subgrp,
                VAR_POP(E) var_of_E,
                (SUM(E * E) /COUNT(E) -
                POWER(AVG(E), 2)) var_of_E2
            FROM
                example_data
            GROUP BY grp , subgrp
        ) AS level2
            ON level1.grp = level2.grp
    GROUP BY level1.grp
    ORDER BY level1.grp
    ''')
    return df
