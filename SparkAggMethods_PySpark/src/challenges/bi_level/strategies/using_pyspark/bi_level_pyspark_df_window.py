import pyspark.sql.functions as func
from pyspark.sql.window import Window
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def bi_level_pyspark_df_window(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    df = data_set.data.df_src
    window = Window \
        .partitionBy(df.grp, df.subgrp) \
        .orderBy(df.id)
    df = (
        df
        .orderBy(df.grp, df.subgrp, df.id)
        .withColumn("sub_var_of_E",
                    func.var_pop(df.E)
                    .over(window))
    )
    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .agg(func.sum(df.C).alias("sub_sum_of_C"),
             func.count(df.C).alias("sub_count"),
             func.max(df.D).alias("sub_max_of_D"),
             func.last(df.sub_var_of_E).alias("sub_var_of_E1"),
             func.var_pop(df.E).alias("sub_var_of_E2"))
    )
    df = (
        df
        .groupBy(df.grp)
        .agg(
            (func.sum(df.sub_sum_of_C) /
             func.sum(df.sub_count)).alias("mean_of_C"),
            func.max(df.sub_max_of_D).alias("max_of_D"),
            func.avg(df.sub_var_of_E1).alias("avg_var_of_E1"),
            func.avg(df.sub_var_of_E2).alias("avg_var_of_E2"))
    )
    df = df.orderBy(df.grp)
    return df
