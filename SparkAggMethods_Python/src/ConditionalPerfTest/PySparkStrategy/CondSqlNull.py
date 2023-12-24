from SixFieldCommon.PySpark_SixFieldTestData import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


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
