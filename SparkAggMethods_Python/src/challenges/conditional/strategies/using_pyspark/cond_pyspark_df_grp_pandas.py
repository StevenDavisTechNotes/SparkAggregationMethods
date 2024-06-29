import math

import pandas as pd

from challenges.conditional.conditional_test_data_types import (
    agg_columns_4, groupby_columns, postAggSchema_4)
from six_field_test_data.six_generate_test_data import (
    DataSetPyspark, TChallengePendingAnswerPythonPyspark)
from six_field_test_data.six_test_data_types import ExecutionParameters
from utils.tidy_spark_session import TidySparkSession


def cond_pyspark_df_grp_pandas(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark,
) -> TChallengePendingAnswerPythonPyspark:
    df = data_set.data.dfSrc

    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, postAggSchema_4)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return df


def my_var(
        column: pd.Series,
) -> float:
    n = len(column)
    return (
        (
            (column * column).sum() / n -
            (column.sum() / n)**2
        )
        if n > 0 else
        math.nan
    )


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    subgroup_key = dfPartition['subgrp'].iloc[0]
    C = dfPartition['C']
    D = dfPartition['D']
    negE = dfPartition[dfPartition["E"] < 0]['E']
    return pd.DataFrame([[
        group_key,
        subgroup_key,
        C.mean(),
        D.max(),
        negE.var(ddof=0),
        negE.agg(my_var),
    ]], columns=groupby_columns + agg_columns_4)
