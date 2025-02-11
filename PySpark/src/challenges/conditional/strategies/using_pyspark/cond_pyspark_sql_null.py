from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def cond_pyspark_sql_null(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if (data_set.data_description.num_source_rows >= 9*10**8):
        return "infeasible", "Too slow"
    spark = spark_session.spark
    spark.catalog.dropTempView("example_data")
    df_src = data_set.data.open_source_data_as_df(spark_session)
    df_src.createTempView("example_data")
    df = spark.sql('''
    SELECT
        grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D,
        VAR_POP(CASE WHEN E < 0 THEN E ELSE NULL END) AS cond_var_of_E
    FROM example_data
    GROUP BY grp, subgrp
    ORDER BY grp, subgrp
    ''')
    return df
