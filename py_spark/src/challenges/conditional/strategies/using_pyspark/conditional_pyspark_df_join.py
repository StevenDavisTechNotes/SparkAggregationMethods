
import pyspark.sql.functions as func
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def conditional_pyspark_df_join(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if (data_set.data_description.num_source_rows >= 9*10**8):
        return "infeasible", "More than 2 hours to run"
    src_df = data_set.data.open_source_data_as_df(spark_session)
    uncond_df = (
        src_df
        .groupBy(src_df.grp, src_df.subgrp)
        .agg(
            func.mean(src_df.C).alias("mean_of_C"),
            func.max(src_df.D).alias("max_of_D"))
    )
    cond_df = (
        src_df
        .filter(src_df.E < 0)
        .groupBy(src_df.grp, src_df.subgrp)
        .agg(
            func.var_pop(src_df.E).alias("cond_var_of_E"))
    )
    df = (
        uncond_df
        .join(cond_df, on=["grp", "subgrp"], how="left")
    )
    df = df.orderBy(df.grp, df.subgrp)
    return df
