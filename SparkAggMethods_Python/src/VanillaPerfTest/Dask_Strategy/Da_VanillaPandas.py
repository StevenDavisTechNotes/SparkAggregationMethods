from typing import Optional, Tuple

import pandas as pd
from dask.bag.core import Bag as dask_bag
from dask.dataframe.core import DataFrame as dask_dataframe
from dask.distributed import Client as DaskClient

from SixFieldCommon.Dask_SixFieldTestData import DaskDataSet
from SixFieldCommon.SixFieldTestData import ExecutionParameters
from VanillaPerfTest.VanillaDataTypes import (dask_post_agg_schema,
                                              result_columns)


def da_vanilla_pandas(
        dask_client: DaskClient,
        _exec_params: ExecutionParameters,
        data_set: DaskDataSet
) -> Tuple[Optional[dask_bag], Optional[dask_dataframe], Optional[pd.DataFrame]]:
    df: dask_dataframe = data_set.data.dfSrc

    df2 = (
        df
        .groupby([df.grp, df.subgrp])
        .apply(inner_agg_method, meta=dask_post_agg_schema)
        # dask.dataframe.groupby.Aggregation
    )
    df3 = df2.compute()
    df3 = df3.sort_values(['grp', 'subgrp']).reset_index(drop=True)
    return None, None, df3


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
