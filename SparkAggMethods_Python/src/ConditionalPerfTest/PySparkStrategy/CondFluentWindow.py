import pyspark.sql.functions as func
from pyspark.sql.window import Window

from SixFieldCommon.PySpark_SixFieldTestData import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from Utils.TidySparkSession import TidySparkSession


def cond_fluent_window(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet,
) -> PysparkPythonPendingAnswerSet:
    df = data_set.data.dfSrc
    df = df \
        .withColumn("cond", func.when(df.E < 0, -1).otherwise(+1))
    df = df \
        .orderBy(df.grp, df.subgrp, df.cond, df.id)
    window = (
        Window
        .partitionBy(df.grp, df.subgrp, df.cond)
        .orderBy(df.id)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df = df \
        .withColumn("cond_var_of_E_2_pre1",
                    func.when(df.cond < 0,
                              func.var_pop(df.E)
                              .over(window))
                    )
    df = (
        df
        .groupBy(df.grp, df.subgrp, df.cond)
        .agg(
            func.sum(df.C).alias("sum_of_C_pre"),
            func.count(df.C).alias("count_of_C_pre"),
            func.max(df.D).alias("max_of_D_pre"),
            func.var_pop(func.when(df.E < 0, df.E)
                         ).alias("cond_var_of_E_1_pre"),
            func.last(df.cond_var_of_E_2_pre1).alias("cond_var_of_E_2_pre2"))
    )

    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .agg(
            (func.sum(df.sum_of_C_pre)
             / func.sum(df.count_of_C_pre)
             ).alias("mean_of_C"),
            func.max(df.max_of_D_pre).alias("max_of_D"),
            func.max(df.cond_var_of_E_1_pre).alias("cond_var_of_E_1"),
            func.max(df.cond_var_of_E_2_pre2).alias("cond_var_of_E_2")
        )
    )
    df = df.orderBy(df.grp, df.subgrp)
    return PysparkPythonPendingAnswerSet(spark_df=df)