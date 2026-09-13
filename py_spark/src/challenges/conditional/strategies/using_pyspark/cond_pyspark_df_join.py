
import pyspark.sql.functions as func
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def cond_pyspark_df_join(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if (data_set.data_description.num_source_rows >= 9*10**8):
        return "infeasible", "More than 2 hours to run"
    df_src = data_set.data.open_source_data_as_df(spark_session)
    df_uncond = (
        df_src
        .groupBy(df_src.grp, df_src.subgrp)
        .agg(
            func.mean(df_src.C).alias("mean_of_C"),
            func.max(df_src.D).alias("max_of_D"))
    )
    df_cond = (
        df_src
        .filter(df_src.E < 0)
        .groupBy(df_src.grp, df_src.subgrp)
        .agg(
            func.var_pop(df_src.E).alias("cond_var_of_E"))
    )
    df = (
        df_uncond
        .join(df_cond, (df_uncond.grp == df_cond.grp) & (df_uncond.subgrp == df_cond.subgrp))
        .drop(df_cond.grp)
        .drop(df_cond.subgrp)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return df
