import pyspark.sql.functions as func

from six_field_test_data.six_generate_test_data import (
    DataSetPyspark, TChallengePendingAnswerPythonPyspark)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.tidy_spark_session import TidySparkSession


def bi_level_pyspark_df_join(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark
) -> TChallengePendingAnswerPythonPyspark:
    df = data_set.data.dfSrc
    df1 = (
        df
        .groupBy(df.grp)
        .agg(
            func.mean(df.C).alias("mean_of_C"),
            func.max(df.D).alias("max_of_D")
        )
    )
    df2 = (
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
    df3 = (
        df2
        .join(df1, "grp")
        .groupBy(df1.grp)
        .agg(
            func.last(df1.mean_of_C).alias("mean_of_C"),
            func.last(df1.max_of_D).alias("max_of_D"),
            func.avg(df2.var_of_E).alias("avg_var_of_E"),
            func.avg(df2.var_of_E2).alias("avg_var_of_E2")
        )
    )
    level4 = df3.orderBy(df3.grp)
    return level4
