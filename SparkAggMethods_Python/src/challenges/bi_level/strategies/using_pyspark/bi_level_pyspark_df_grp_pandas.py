import pandas as pd

from challenges.bi_level.bi_level_test_data_types import (postAggSchema,
                                                          result_columns)
from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, TChallengePendingAnswerPythonPyspark)
from six_field_test_data.six_test_data_types import ExecutionParameters
from t_utils.tidy_spark_session import TidySparkSession


def bi_level_pyspark_df_grp_pandas(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> TChallengePendingAnswerPythonPyspark:
    df = data_set.data.dfSrc

    df = (
        df
        .groupBy(df.grp)
        .applyInPandas(inner_agg_method, postAggSchema)
    )
    df = df.orderBy(df.grp)
    return df


def inner_agg_method(
        dfPartition: pd.DataFrame
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    col_c = dfPartition['C']
    col_d = dfPartition['D']
    sub_group_e = dfPartition.groupby('subgrp')['E']
    return pd.DataFrame([[
        group_key,
        col_c.mean(),
        col_d.max(),
        sub_group_e.var(ddof=0).mean(),
        sub_group_e
        .agg(lambda col_e:
             ((col_e * col_e).sum() / col_e.count() -
              (col_e.sum() / col_e.count())**2))
        .mean(),
    ]], columns=result_columns)
