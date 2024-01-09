import pandas as pd

from challenges.bi_level.bi_level_test_data_types import (postAggSchema,
                                                          result_columns)
from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, TPysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.tidy_spark_session import TidySparkSession


def bi_level_pyspark_df_grp_pandas(
        _spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> TPysparkPythonPendingAnswerSet:
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
    C = dfPartition['C']
    D = dfPartition['D']
    subgroupedE = dfPartition.groupby('subgrp')['E']
    return pd.DataFrame([[
        group_key,
        C.mean(),
        D.max(),
        subgroupedE.var(ddof=0).mean(),
        subgroupedE
        .agg(lambda E:
             ((E * E).sum() / E.count() -
              (E.sum() / E.count())**2))
        .mean(),
    ]], columns=result_columns)
