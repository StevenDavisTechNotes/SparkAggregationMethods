import pandas as pd
from dask.dataframe.core import DataFrame as DaskDataFrame

from src.challenges.vanilla.vanilla_test_data_types import (
    dask_post_agg_schema, result_columns)
from src.six_field_test_data.six_generate_test_data import (
    DataSetDask, TChallengeAnswerPythonDask)
from src.six_field_test_data.six_test_data_types import ExecutionParameters


def vanilla_dask_ddf_grp_apply(
        exec_params: ExecutionParameters,
        data_set: DataSetDask
) -> TChallengeAnswerPythonDask:
    if (data_set.data_size.points_per_index > 10**4):
        return "infeasible"
    df: DaskDataFrame = data_set.data.df_src
    df2 = (
        df
        .groupby([df.grp, df.subgrp])
        .apply(inner_agg_method, meta=dask_post_agg_schema)
    )
    df3 = (
        df2.compute()
        .sort_values(["grp", "subgrp"])
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
    ]], columns=result_columns)
