from six_field_test_data.six_generate_test_data import (
    DataSetPyspark, TChallengePendingAnswerPythonPyspark)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.tidy_spark_session import TidySparkSession


def cond_pyspark_sql_null(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark,
) -> TChallengePendingAnswerPythonPyspark:
    spark = spark_session.spark
    spark.catalog.dropTempView("example_data")
    data_set.data.dfSrc.createTempView("example_data")
    df = spark.sql('''
    SELECT
        grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D,
        VAR_POP(CASE WHEN E < 0 THEN E ELSE NULL END) AS cond_var_of_E
    FROM example_data
    GROUP BY grp, subgrp
    ORDER BY grp, subgrp
    ''')
    return df
