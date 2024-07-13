import numpy
import pandas as pd
from dask.dataframe import groupby
from dask.dataframe.core import DataFrame as DaskDataFrame
from pandas.core.groupby.generic import SeriesGroupBy

from src.challenges.vanilla.vanilla_test_data_types import agg_columns
from src.six_field_test_data.six_generate_test_data import (
    DataSetDask, TChallengeAnswerPythonDask)
from src.six_field_test_data.six_test_data_types import ExecutionParameters


def ddof_0_do_chunk(s: SeriesGroupBy) -> tuple[pd.Series, pd.Series, pd.Series]:
    return s.count(), s.sum(), s.apply(
        lambda r: numpy.sum(numpy.power(r, 2)))


def ddof_0_do_agg(count: SeriesGroupBy, sum: SeriesGroupBy, sum2: SeriesGroupBy
                  ) -> tuple[pd.Series, pd.Series, pd.Series]:
    return count.sum(), sum.sum(), sum2.sum()


def ddof_0_do_finalize(count: pd.Series, sum: pd.Series, sum2: pd.Series):
    return sum2 / count - (sum / count)**2


def vanilla_dask_ddf_grp_udaf(
        exec_params: ExecutionParameters,
        data_set: DataSetDask
) -> TChallengeAnswerPythonDask:
    if (data_set.data_size.points_per_index > 10**4):
        return "infeasible"
    df: DaskDataFrame = data_set.data.df_src
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
            # var_of_E=('E', 'var(ddof=0)'),  # doesn't work
            var_of_E2=('E', custom_var_ddof_0),
        )
    )
    dd2_just_var_of_E = dd_grouped["E"].var(ddof=0).to_frame('var_of_E')
    df2 = pd.merge(
        left=dd2_main.compute(),
        right=dd2_just_var_of_E.compute(),
        on=['grp', 'subgrp'], how='inner')
    df3 = (
        df2.loc[:, agg_columns]
        .sort_index()
        .reset_index(drop=False)
    )
    return df3
