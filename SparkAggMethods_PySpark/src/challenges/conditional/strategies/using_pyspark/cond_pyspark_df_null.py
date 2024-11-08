import pyspark.sql.functions as func
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def cond_pyspark_df_null(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    df_src = data_set.data.open_source_data_as_df(spark_session)
    df_inter = (
        df_src
        .groupBy(df_src.grp, df_src.subgrp)
        .agg(func.mean(df_src.C).alias("mean_of_C"),
             func.max(df_src.D).alias("max_of_D"),
             func.var_pop(func.when(df_src.E < 0, df_src.E))
             .alias("cond_var_of_E"))
    )
    df = df_inter.select('grp', 'subgrp', 'mean_of_C', 'max_of_D', 'cond_var_of_E')
    df = df.orderBy(df.grp, df.subgrp)
    return df
