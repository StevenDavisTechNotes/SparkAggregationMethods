
import pyspark.sql.functions as func
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def cond_pyspark_df_nested(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if (data_set.data_description.num_source_rows >= 9*10**8):
        return "infeasible", "More than 2 hours to run"
    df_src = data_set.data.open_source_data_as_df(spark_session)
    df_inter = df_src\
        .withColumn('cond', func.when(df_src.E < 0, -1).otherwise(1))
    df_inter = (
        df_inter
        .groupBy(df_inter.grp, df_inter.subgrp, df_inter.cond)
        .agg(func.mean(df_src.C).alias("sub_mean_of_C"),
             func.count(df_src.C).alias("sub_count"),
             func.sum(df_src.C).alias("sub_sum_of_C"),
             func.max(df_src.D).alias("sub_max_of_D"),
             func.var_pop(df_src.E).alias("sub_var_of_E"),
             func.sum(df_src.E * df_src.E).alias("sub_sum_of_E_squared"),
             func.sum(df_src.E).alias("sub_sum_of_E")))
    df_inter = (
        df_inter
        .groupBy(df_inter.grp, df_inter.subgrp)
        .agg(func.mean(df_inter.sub_mean_of_C).alias("wrong_mean_of_C"),
             (
            func.sum(df_inter.sub_mean_of_C * df_inter.sub_count)
            / func.sum(df_inter.sub_count)
        ).alias("mean_of_C2"),
            func.sum(df_inter.sub_count).alias("uncond_count"),
            func.sum(func.when(df_inter.cond < 0, df_inter.sub_count)
                     .otherwise(0)).alias("cond_count"),
            func.sum(df_inter.sub_sum_of_C).alias("sum_of_C"),
            func.max(df_inter.sub_max_of_D).alias("max_of_D"),
            func.sum(func.when(df_inter.cond < 0, df_inter.sub_var_of_E)
                     .otherwise(0)).alias("cond_var_of_E"))
    )
    df_inter = df_inter \
        .withColumn('mean_of_C', df_inter.sum_of_C / df_inter.uncond_count)
    df_result = df_inter.select('grp', 'subgrp', 'mean_of_C', 'mean_of_C2', 'wrong_mean_of_C',
                                'max_of_D', 'cond_var_of_E')
    df_result = df_result.orderBy(df_result.grp, df_result.subgrp)
    return df_result
