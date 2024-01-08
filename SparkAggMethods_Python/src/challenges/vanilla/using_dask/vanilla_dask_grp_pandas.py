import pandas as pd
from dask.dataframe.core import DataFrame as dask_dataframe
from dask.distributed import Client as DaskClient

from challenges.vanilla.vanilla_test_data_types import (dask_post_agg_schema,
                                                        result_columns)
from six_field_test_data.six_generate_test_data_using_dask import (
    DaskDataSet, DaskPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import ExecutionParameters


def da_vanilla_pandas(
        dask_client: DaskClient,
        _exec_params: ExecutionParameters,
        data_set: DaskDataSet
) -> DaskPythonPendingAnswerSet:
    if (
        data_set.description.NumDataPoints
        // data_set.description.NumGroups
        // data_set.description.NumSubGroups
        > 10**4
    ):
        return DaskPythonPendingAnswerSet(feasible=False)
    df: dask_dataframe = data_set.data.dfSrc
    df2 = (
        df
        .groupby([df.grp, df.subgrp])
        .apply(inner_agg_method, meta=dask_post_agg_schema)
    )
    df3 = (
        df2.compute()
        .sort_index()
        .reset_index(drop=True)
    )
    return DaskPythonPendingAnswerSet(panda_df=df3)


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
