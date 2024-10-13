import pyspark.sql.functions as func

from src.six_field_test_data.six_generate_test_data import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.six_field_test_data.six_test_data_types import SixTestExecutionParameters
from src.utils.tidy_spark_session import TidySparkSession


def vanilla_pyspark_df_grp_builtin(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    df = data_set.data.df_src
    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .agg(
            func.mean(df.C).alias("mean_of_C"),
            func.max(df.D).alias("max_of_D"),
            func.var_pop(df.E).alias("var_of_E"),
            (
                func.sum(df.E * df.E) / func.count(df.E)
                - func.pow(func.sum(df.E) / func.count(df.E), 2)
            ).alias("var_of_E2")
        )
        .orderBy(df.grp, df.subgrp))
    return df
