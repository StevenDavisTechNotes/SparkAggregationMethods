from typing import cast

import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import BI_LEVEL_RESULT_COLUMNS
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.bi_level.bi_level_test_data_types_pyspark import PysparkPostAggregationSchema
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def bi_level_pyspark_df_grp_pandas_numpy(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    df = data_set.data.open_source_data_as_df(spark_session)
    df = (
        df
        .groupBy(df.grp)
        .applyInPandas(inner_agg_method, PysparkPostAggregationSchema)
    )
    df = df.orderBy(df.grp)
    return df


def my_loop_lift_var(x: pd.Series) -> float:
    return numpy.inner(x, x) / x.count() - (numpy.sum(x) / x.count())**2


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition["grp"].iloc[0]
    C = dfPartition["C"]
    D = dfPartition["D"]
    sub_group_E = dfPartition.groupby("subgrp")["E"]
    return pd.DataFrame(
        [[
            group_key,
            numpy.mean(C),
            numpy.max(D),
            sub_group_E.apply(lambda x: numpy.var(x)).mean(),
            sub_group_E.apply(my_loop_lift_var).mean(),
        ]],
        columns=cast(pd.Index, BI_LEVEL_RESULT_COLUMNS),
    )
