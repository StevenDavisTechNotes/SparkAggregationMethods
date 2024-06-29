import pyspark.sql.functions as func

from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, TChallengePendingAnswerPythonPyspark)
from six_field_test_data.six_test_data_types import ExecutionParameters
from t_utils.tidy_spark_session import TidySparkSession


def cond_pyspark_df_zero(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: PysparkDataSet,
) -> TChallengePendingAnswerPythonPyspark:
    dfData = data_set.data.dfSrc
    df = (
        dfData
        .groupBy(dfData.grp, dfData.subgrp)
        .agg(func.mean(dfData.C).alias("mean_of_C"),
             func.max(dfData.D).alias("max_of_D"),
             func.sum(func.when(dfData.E < 0, dfData.E * dfData.E)
                      .otherwise(0)).alias("cond_sum_of_E_squared"),
             func.sum(func.when(dfData.E < 0, dfData.E)
                      .otherwise(0)).alias("cond_sum_of_E"),
             func.sum(func.when(dfData.E < 0, 1)
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
