from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, TPysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.tidy_spark_session import TidySparkSession


def vanilla_sql(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> TPysparkPythonPendingAnswerSet:
    df = data_set.data.dfSrc
    spark_session.spark.catalog.dropTempView("exampledata")
    df.createTempView("exampledata")
    df = spark_session.spark.sql('''
    SELECT
        grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D,
        VAR_POP(E) var_of_E,
        (
            SUM(E * E) / COUNT(E) -
            POWER(SUM(E) / COUNT(E), 2)
        ) var_of_E2
    FROM
        exampledata
    GROUP BY grp, subgrp
    ORDER BY grp, subgrp
    ''')
    return df
