import pyspark.sql.functions as func

from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.TidySparkSession import TidySparkSession


def bi_fluent_join(
        _spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> PysparkPythonPendingAnswerSet:
    df = data_set.data.dfSrc
    level1 = (
        df
        .groupBy(df.grp)
        .agg(
            func.mean(df.C).alias("mean_of_C"),
            func.max(df.D).alias("max_of_D")
        )
    )
    level2 = (
        df
        .groupBy(df.grp, df.subgrp)
        .agg(
            func.var_pop(df.E).alias("var_of_E"),
            (
                func.sum(df.E * df.E) / func.count(df.E) -
                func.avg(df.E)**2
            ).alias("var_of_E2")
        )
    )
    level3 = (
        level2
        .join(level1, "grp")
        .groupBy(level1.grp)
        .agg(
            func.last(level1.mean_of_C).alias("mean_of_C"),
            func.last(level1.max_of_D).alias("max_of_D"),
            func.avg(level2.var_of_E).alias("avg_var_of_E"),
            func.avg(level2.var_of_E2).alias("avg_var_of_E2")
        )
    )
    level4 = level3.orderBy(level3.grp)
    return PysparkPythonPendingAnswerSet(spark_df=level4)
