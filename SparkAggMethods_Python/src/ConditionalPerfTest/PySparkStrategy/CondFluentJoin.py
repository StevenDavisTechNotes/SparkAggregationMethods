
import pyspark.sql.functions as func

from SixFieldCommon.PySpark_SixFieldTestData import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def cond_fluent_join(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: PysparkDataSet,
) -> PysparkPythonPendingAnswerSet:
    dfData = data_set.data.dfSrc
    uncond = (
        dfData
        .groupBy(dfData.grp, dfData.subgrp)
        .agg(
            func.mean(dfData.C).alias("mean_of_C"),
            func.max(dfData.D).alias("max_of_D"))
    )
    cond = (
        dfData
        .filter(dfData.E < 0)
        .groupBy(dfData.grp, dfData.subgrp)
        .agg(
            func.var_pop(dfData.E).alias("cond_var_of_E"))
    )
    df = (
        uncond
        .join(cond, (uncond.grp == cond.grp) & (uncond.subgrp == cond.subgrp))
        .drop(cond.grp)
        .drop(cond.subgrp)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return PysparkPythonPendingAnswerSet(spark_df=df)
