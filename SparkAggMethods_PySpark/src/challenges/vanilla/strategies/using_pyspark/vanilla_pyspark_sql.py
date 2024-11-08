from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def vanilla_pyspark_sql(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if (data_set.data_description.num_source_rows >= 9*10**6):  # incorrect answer at that point
        return "infeasible"
    df = data_set.data.open_source_data_as_df(spark_session)
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
