from typing import cast

import pandas as pd
from dask.dataframe import DataFrame as DaskDataFrame
from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import GROUP_BY_COLUMNS
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.bi_level.bi_level_test_data_types_dask import POST_AGG_SCHEMA_NO_INDEX_DASK
from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask


def bi_level_dask_ddf_grp_apply(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask
) -> TChallengeAnswerPythonDask:
    ddf = data_set.data.open_source_data_as_ddf()
    ddf = cast(
        DaskDataFrame,
        ddf
        .groupby(ddf.grp)
        .apply(agg_method_per_group, meta=POST_AGG_SCHEMA_NO_INDEX_DASK)
    )
    pdf = (
        ddf.compute()
        .reset_index(drop=False)
        .sort_values(GROUP_BY_COLUMNS)
        .reset_index(drop=True)
    )
    return pdf


def agg_method_per_group(
        df: pd.DataFrame,
) -> pd.Series:
    col_c = df["C"]
    col_d = df["D"]
    sub_group_e = df.groupby("subgrp")["E"]
    var_by_sub_group = cast(pd.Series, sub_group_e.var(ddof=0))
    return pd.Series({
        "mean_of_C": col_c.mean(),
        "max_of_D": col_d.max(),
        "avg_var_of_E": var_by_sub_group.mean(),
        "avg_var_of_E2": sub_group_e.agg(agg_method_per_sub_group).mean(),
    })


def agg_method_per_sub_group(
        col_e: pd.Series,
) -> float:
    return (
        (col_e * col_e).sum() / col_e.count()
        - (col_e.sum() / col_e.count())**2
    )
