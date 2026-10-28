import math
from typing import cast

import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.conditional.conditional_test_data_types import (
    AGGREGATION_COLUMNS_4, GROUP_BY_COLUMNS,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.conditional.conditional_test_data_types_pyspark import POST_AGGREGATION_SCHEMA_4
from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def cond_pyspark_df_grp_pandas_numpy(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if (data_set.data_description.num_source_rows >= 9*10**7):  # panda dataframe must fit in memory, get socket timeout
        return "infeasible", "Whole Panda dataframe must fit in memory"

    df = data_set.data.open_source_data_as_df(spark_session)
    df = (
        df
        .groupby(df.grp, df.subgrp)
        .applyInPandas(inner_agg_method, POST_AGGREGATION_SCHEMA_4)
    )
    df = df.orderBy(df.grp, df.subgrp)
    return df


def my_loop_lift_var(
        x: pd.Series,
) -> float:
    n = x.count()
    return numpy.inner(x, x) / n - (numpy.sum(x) / n)**2 if n > 0 else math.nan


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.DataFrame:
    group_key = dfPartition["grp"].iloc[0]
    subgroup_key = dfPartition["subgrp"].iloc[0]
    C = dfPartition["C"]
    D = dfPartition["D"]
    negE = dfPartition.loc[dfPartition["E"] < 0, "E"]
    return pd.DataFrame(
        [[
            group_key,
            subgroup_key,
            numpy.mean(C),
            numpy.max(D),
            numpy.var(negE),
            my_loop_lift_var(negE),
        ]],
        columns=cast(pd.Index, GROUP_BY_COLUMNS + AGGREGATION_COLUMNS_4),
    )
