import pyspark.sql.functions as func

from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.tidy_spark_session import TidySparkSession


def vanilla_pyspark_df_grp_builtin(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> PysparkPythonPendingAnswerSet:
    df = data_set.data.dfSrc
    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .agg(
            func.mean(df.C).alias("mean_of_C"),
            func.max(df.D).alias("max_of_D"),
            func.var_pop(df.E).alias("var_of_E"),
            (
                func.sum(df.E * df.E) / func.count(df.E)
                - func.pow(func.sum(df.E) / func.count(df.E), 2)
            ).alias("var_of_E2")
        )
        .orderBy(df.grp, df.subgrp))
    return PysparkPythonPendingAnswerSet(spark_df=df)
