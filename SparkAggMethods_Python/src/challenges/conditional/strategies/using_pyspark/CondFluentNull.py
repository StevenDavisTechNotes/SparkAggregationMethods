import pyspark.sql.functions as func

from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.TidySparkSession import TidySparkSession


def cond_fluent_null(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet,
) -> PysparkPythonPendingAnswerSet:
    dfData = data_set.data.dfSrc
    dfInter = (
        dfData
        .groupBy(dfData.grp, dfData.subgrp)
        .agg(func.mean(dfData.C).alias("mean_of_C"),
             func.max(dfData.D).alias("max_of_D"),
             func.var_pop(func.when(dfData.E < 0, dfData.E))
             .alias("cond_var_of_E"))
    )
    df = dfInter.select('grp', 'subgrp', 'mean_of_C', 'max_of_D', 'cond_var_of_E')
    df = df.orderBy(df.grp, df.subgrp)
    return PysparkPythonPendingAnswerSet(spark_df=df)