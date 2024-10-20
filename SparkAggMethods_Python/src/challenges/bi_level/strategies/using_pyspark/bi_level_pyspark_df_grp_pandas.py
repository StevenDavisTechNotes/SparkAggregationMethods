from typing import cast

import pandas as pd
from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import BI_LEVEL_RESULT_COLUMNS
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from challenges.bi_level.bi_level_test_data_types_pyspark import PysparkPostAggregationSchema
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_spark_session import TidySparkSession


def bi_level_pyspark_df_grp_pandas(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    df = data_set.data.df_src

    df = (
        df
        .groupBy(df.grp)
        .applyInPandas(inner_agg_method, PysparkPostAggregationSchema)
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
        cast(pd.Series,
             sub_group_e
             .agg(lambda col_e:
                  ((col_e * col_e).sum() / col_e.count() -
                   (col_e.sum() / col_e.count())**2)))
        .mean(),
    ]], columns=BI_LEVEL_RESULT_COLUMNS)
