
import pyspark.sql.functions as func
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_spark_session import TidySparkSession


def cond_pyspark_df_join(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    dfData = data_set.data.df_src
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
    return df
