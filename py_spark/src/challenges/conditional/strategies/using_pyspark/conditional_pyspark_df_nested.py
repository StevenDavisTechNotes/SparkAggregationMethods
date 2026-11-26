
import pyspark.sql.functions as func
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def conditional_pyspark_df_nested(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if (data_set.data_description.num_source_rows >= 9*10**8):
        return "infeasible", "More than 2 hours to run"
    src_df = data_set.data.open_source_data_as_df(spark_session)
    inter_df = src_df\
        .withColumn("cond", func.when(src_df.E < 0, -1).otherwise(1))
    inter_df = (
        inter_df
        .groupBy(inter_df.grp, inter_df.subgrp, inter_df.cond)
        .agg(func.mean(src_df.C).alias("sub_mean_of_C"),
             func.count(src_df.C).alias("sub_count"),
             func.sum(src_df.C).alias("sub_sum_of_C"),
             func.max(src_df.D).alias("sub_max_of_D"),
             func.var_pop(src_df.E).alias("sub_var_of_E"),
             func.sum(src_df.E * src_df.E).alias("sub_sum_of_E_squared"),
             func.sum(src_df.E).alias("sub_sum_of_E")))
    inter_df = (
        inter_df
        .groupBy(inter_df.grp, inter_df.subgrp)
        .agg(func.mean(inter_df.sub_mean_of_C).alias("wrong_mean_of_C"),
             (
            func.sum(inter_df.sub_mean_of_C * inter_df.sub_count)
            / func.sum(inter_df.sub_count)
        ).alias("mean_of_C2"),
            func.sum(inter_df.sub_count).alias("uncond_count"),
            func.sum(func.when(inter_df.cond < 0, inter_df.sub_count)
                     .otherwise(0)).alias("cond_count"),
            func.sum(inter_df.sub_sum_of_C).alias("sum_of_C"),
            func.max(inter_df.sub_max_of_D).alias("max_of_D"),
            func.sum(func.when(inter_df.cond < 0, inter_df.sub_var_of_E)
                     .otherwise(0)).alias("cond_var_of_E"))
    )
    inter_df = inter_df \
        .withColumn("mean_of_C", inter_df.sum_of_C / inter_df.uncond_count)
    result_df = inter_df.select("grp", "subgrp", "mean_of_C", "mean_of_C2", "wrong_mean_of_C",
                                "max_of_D", "cond_var_of_E")
    result_df = result_df.orderBy(result_df.grp, result_df.subgrp)
    return result_df
