import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.challenges.vanilla.vanilla_test_data_types import VANILLA_RESULT_COLUMNS, pyspark_post_agg_schema
from src.utils.tidy_spark_session import TidySparkSession


def vanilla_pyspark_df_grp_numpy(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:

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
    return pd.DataFrame([[
        group_key,
        subgroup_key,
        numpy.mean(C),
        numpy.max(D),
        numpy.var(E),
        numpy.inner(E, E) / E.count()
        - (numpy.sum(E) / E.count())**2,  # type: ignore
    ]], columns=VANILLA_RESULT_COLUMNS)
