from src.six_field_test_data.six_generate_test_data import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.six_field_test_data.six_test_data_types import SixTestExecutionParameters
from src.utils.tidy_spark_session import TidySparkSession


def bi_level_pyspark_sql_nested(
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
            grp,
            SUM(sub_sum_of_C) / SUM(sub_count) as mean_of_C,
            MAX(sub_max_of_D) as max_of_D,
            AVG(sub_var_of_E) as avg_var_of_E,
            AVG(
                sub_sum_of_E_squared / sub_count -
                POWER(sub_sum_of_E / sub_count, 2)
               ) as avg_var_of_E2
    FROM
        (SELECT
                grp, subgrp,
                count(C) as sub_count,
                sum(C) as sub_sum_of_C,
                max(D) as sub_max_of_D,
                VAR_POP(E) as sub_var_of_E,
                sum(E * E) as sub_sum_of_E_squared,
                sum(E) as sub_sum_of_E
            FROM
                example_data
            GROUP BY grp, subgrp) level2
    GROUP BY grp
    ORDER BY grp
    ''')
    return df
