from src.six_field_test_data.six_generate_test_data import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.six_field_test_data.six_test_data_types import SixTestExecutionParameters
from src.utils.tidy_spark_session import TidySparkSession


def vanilla_pyspark_sql(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    df = data_set.data.df_src
    spark_session.spark.catalog.dropTempView("example_data")
    df.createTempView("example_data")
    df = spark_session.spark.sql('''
    SELECT
        grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D,
        VAR_POP(E) var_of_E,
        (
            SUM(E * E) / COUNT(E) -
            POWER(SUM(E) / COUNT(E), 2)
        ) var_of_E2
    FROM
        example_data
    GROUP BY grp, subgrp
    ORDER BY grp, subgrp
    ''')
    return df
