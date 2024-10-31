# pyright: basic, reportArgumentType=false

import pandas as pd
from dask.dataframe.core import DataFrame as DaskDataFrame
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    GROUP_BY_COLUMNS, VANILLA_RESULT_COLUMNS,
)

from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask
from src.challenges.vanilla.vanilla_test_data_types_dask import dask_post_agg_schema


def vanilla_dask_ddf_grp_apply(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask
) -> TChallengeAnswerPythonDask:
    if (data_set.data_description.points_per_index > 10**4):
        return "infeasible"
    df: DaskDataFrame = data_set.data.df_src
    df2 = (
        df
        .groupby([df.grp, df.subgrp])
        .apply(inner_agg_method, meta=dask_post_agg_schema)
    )
    df3 = (
        df2.compute()
        .sort_values(GROUP_BY_COLUMNS)
        .reset_index(drop=True)
    )
    return df3


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
    ]], columns=VANILLA_RESULT_COLUMNS)
