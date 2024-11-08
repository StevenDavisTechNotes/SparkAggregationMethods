from typing import cast

import pandas as pd
from dask.dataframe.core import DataFrame as DaskDataFrame
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import GROUP_BY_COLUMNS

from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask
from src.challenges.vanilla.vanilla_test_data_types_dask import POST_AGG_SCHEMA_NO_INDEX_DASK


def vanilla_dask_ddf_grp_apply(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask
) -> TChallengeAnswerPythonDask:
    if (data_set.data_description.points_per_index > 10**4):
        return "infeasible"
    ddf = data_set.data.open_source_data_as_ddf()
    ddf = cast(
        DaskDataFrame,
        ddf
        .groupby([ddf.grp, ddf.subgrp])
        .apply(inner_agg_method, meta=POST_AGG_SCHEMA_NO_INDEX_DASK)
    )
    pdf = (
        ddf.compute()
        .reset_index(drop=False)
        .sort_values(GROUP_BY_COLUMNS)
        .reset_index(drop=True)
    )
    return pdf


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.Series:
    C = dfPartition['C']
    D = dfPartition['D']
    E = dfPartition['E']
    var_of_E2 = (
        (E * E).sum() / E.count()
        - (E.sum() / E.count())**2)
    return pd.Series({
        'mean_of_C': C.mean(),
        'max_of_D': D.max(),
        'var_of_E': E.var(ddof=0),
        'var_of_E2': var_of_E2,
    })
