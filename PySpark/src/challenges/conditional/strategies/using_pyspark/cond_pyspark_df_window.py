import pyspark.sql.functions as func
from pyspark.sql.window import Window
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def cond_pyspark_df_window(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    # at 9*10**7 Low on memory, WARN RowBasedKeyValueBatch: Calling spill() on RowBasedKeyValueBatch.
    if (data_set.data_description.num_source_rows >= 9*10**7):
        return "infeasible", "Low on memory, unable to spill since RowBasedKeyValueBatch"
    df = data_set.data.open_source_data_as_df(spark_session)
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
            func.max(df.cond_var_of_E_1_pre).alias("cond_var_of_E"),
            func.max(df.cond_var_of_E_2_pre2).alias("cond_var_of_E2")
        )
    )
    df = df.orderBy(df.grp, df.subgrp)
    return df
