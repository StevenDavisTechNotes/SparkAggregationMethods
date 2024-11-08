import pyspark.sql.functions as func
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def cond_pyspark_df_zero(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    df_src = data_set.data.open_source_data_as_df(spark_session)
    df = (
        df_src
        .groupBy(df_src.grp, df_src.subgrp)
        .agg(func.mean(df_src.C).alias("mean_of_C"),
             func.max(df_src.D).alias("max_of_D"),
             func.sum(func.when(df_src.E < 0, df_src.E * df_src.E)
                      .otherwise(0)).alias("cond_sum_of_E_squared"),
             func.sum(func.when(df_src.E < 0, df_src.E)
                      .otherwise(0)).alias("cond_sum_of_E"),
             func.sum(func.when(df_src.E < 0, 1)
                      .otherwise(0)).alias("cond_count"))
    )
    df = df\
        .withColumn("cond_var_of_E",
                    func.when(df.cond_count > 0,
                              df.cond_sum_of_E_squared / df.cond_count
                              - (df.cond_sum_of_E / df.cond_count)**2))
    df = df\
        .select('grp', 'subgrp', 'mean_of_C', 'max_of_D', 'cond_var_of_E')
    df = df\
        .orderBy(df.grp, df.subgrp)
    return df
