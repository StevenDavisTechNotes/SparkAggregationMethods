import math

import pandas as pd

from src.challenges.conditional.conditional_test_data_types import (
    AGG_COLUMN_NAMES_4, GROUP_BY_COLUMNS, POST_AGG_SCHEMA_4)
from src.six_field_test_data.six_generate_test_data import (
    DataSetPyspark, TChallengePendingAnswerPythonPyspark)
from src.six_field_test_data.six_test_data_types import ExecutionParameters
from src.utils.tidy_spark_session import TidySparkSession


def cond_pyspark_df_grp_pandas(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark,
) -> TChallengePendingAnswerPythonPyspark:
    df = data_set.data.df_src

    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, POST_AGG_SCHEMA_4)
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
    ]], columns=GROUP_BY_COLUMNS + AGG_COLUMN_NAMES_4)
