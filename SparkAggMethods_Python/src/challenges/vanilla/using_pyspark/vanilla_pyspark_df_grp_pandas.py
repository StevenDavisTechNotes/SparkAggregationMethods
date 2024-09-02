import pandas as pd

from src.challenges.vanilla.vanilla_test_data_types import (
    RESULT_COLUMNS, pyspark_post_agg_schema)
from src.six_field_test_data.six_generate_test_data import (
    DataSetPyspark, TChallengePendingAnswerPythonPyspark)
from src.six_field_test_data.six_test_data_types import ExecutionParameters
from src.utils.tidy_spark_session import TidySparkSession


def vanilla_pyspark_df_grp_pandas(
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        data_set: DataSetPyspark
) -> TChallengePendingAnswerPythonPyspark:
    df = data_set.data.df_src

    df = (
        df
        .groupBy(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, pyspark_post_agg_schema)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return df


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition['grp'].iloc[0]
    subgroup_key = dfPartition['subgrp'].iloc[0]
    C = dfPartition['C']
    D = dfPartition['D']
    E = dfPartition['E']
    var_of_E2 = (
        (E * E).sum() / E.count()
        - (E.sum() / E.count())**2)
    return pd.DataFrame([[
        group_key,
        subgroup_key,
        C.mean(),
        D.max(),
        E.var(ddof=0),
        var_of_E2,
    ]], columns=RESULT_COLUMNS)
