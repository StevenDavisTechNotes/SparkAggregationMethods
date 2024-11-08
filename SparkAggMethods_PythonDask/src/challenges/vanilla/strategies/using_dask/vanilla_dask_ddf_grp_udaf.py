from typing import cast

import numpy
import pandas as pd
from dask.dataframe import groupby
from dask.dataframe.core import DataFrame as DaskDataFrame
from pandas.core.groupby.generic import SeriesGroupBy
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import GROUP_BY_COLUMNS

from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask


def ddof_0_do_chunk(s: SeriesGroupBy) -> tuple[pd.Series, pd.Series, pd.Series]:
    return (
        cast(pd.Series, s.count()),
        cast(pd.Series, s.sum()),
        cast(pd.Series, s.apply(lambda r: numpy.sum(numpy.power(r, 2)))),
    )


def ddof_0_do_agg(
    count: SeriesGroupBy, sum: SeriesGroupBy, sum2: SeriesGroupBy
) -> tuple[pd.Series, pd.Series, pd.Series]:
    return (
        cast(pd.Series, count.sum()),
        cast(pd.Series, sum.sum()),
        cast(pd.Series, sum2.sum()),
    )


def ddof_0_do_finalize(count: pd.Series, sum: pd.Series, sum2: pd.Series):
    return sum2 / count - (sum / count)**2


def vanilla_dask_ddf_grp_udaf(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask
) -> TChallengeAnswerPythonDask:
    if (data_set.data_description.points_per_index > 10**4):
        return "infeasible"
    df: DaskDataFrame = data_set.data.open_source_data_as_ddf()
    custom_var_ddof_0 = groupby.Aggregation(
        name='custom_var_ddof_0',
        chunk=ddof_0_do_chunk,
        agg=ddof_0_do_agg,
        finalize=ddof_0_do_finalize,
    )
    dd_grouped = df.groupby([df.grp, df.subgrp])
    dd2_main = (
        dd_grouped
        .aggregate(
            mean_of_C=('C', 'mean'),
            max_of_D=('D', 'max'),
            # var_of_E=('E', 'var(ddof=0)'),  # doesn't respect ddof=0
            var_of_E2=('E', custom_var_ddof_0),
        )
    )
    dd2_just_var_of_E = dd_grouped["E"].var(ddof=0).to_frame('var_of_E')
    df2 = (
        pd.merge(
            left=dd2_main.compute(),
            right=dd2_just_var_of_E.compute(),
            on=GROUP_BY_COLUMNS,
            how='inner',
        )
        .reset_index(drop=False)
    )
    df3 = (
        df2
        .sort_values(GROUP_BY_COLUMNS)
        .reset_index(drop=True)
    )
    return df3
