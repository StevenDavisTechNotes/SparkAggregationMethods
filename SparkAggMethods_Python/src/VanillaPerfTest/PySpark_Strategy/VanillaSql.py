from SixFieldCommon.PySpark_SixFieldTestData import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def vanilla_sql(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> PysparkPythonPendingAnswerSet:
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
    return PysparkPythonPendingAnswerSet(spark_df=df)
