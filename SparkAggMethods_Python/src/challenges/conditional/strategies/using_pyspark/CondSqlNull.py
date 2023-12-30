from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.TidySparkSession import TidySparkSession


def cond_sql_null(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet,
) -> PysparkPythonPendingAnswerSet:
    spark = spark_session.spark
    spark.catalog.dropTempView("exampledata")
    data_set.data.dfSrc.createTempView("exampledata")
    df = spark.sql('''
    SELECT
        grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D,
        VAR_POP(CASE WHEN E < 0 THEN E ELSE NULL END) AS cond_var_of_E
    FROM exampledata
    GROUP BY grp, subgrp
    ORDER BY grp, subgrp
    ''')
    return PysparkPythonPendingAnswerSet(spark_df=df)
