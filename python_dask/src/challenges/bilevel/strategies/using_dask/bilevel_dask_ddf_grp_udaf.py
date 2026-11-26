from typing import cast

import numpy
import pandas as pd
from dask.dataframe import DataFrame as DaskDataFrame
from dask.dataframe import groupby
from pandas.core.groupby.generic import SeriesGroupBy
from spark_agg_methods_common_python.challenges.bilevel.bilevel_test_data_types import GROUP_BY_COLUMNS
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask


def ddof_0_do_chunk(s: SeriesGroupBy) -> tuple[pd.Series, pd.Series, pd.Series]:
    return (
        s.count(),
        cast(pd.Series, s.sum()),
        s.apply(lambda r: numpy.sum(numpy.power(r, 2))),
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


def bilevel_dask_ddf_grp_udaf(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask
) -> TChallengeAnswerPythonDask:
    source_ddf: DaskDataFrame = data_set.data.open_source_data_as_ddf()
    custom_var_ddof_0 = groupby.Aggregation(
        name="custom_var_ddof_0",
        chunk=ddof_0_do_chunk,
        agg=ddof_0_do_agg,
        finalize=ddof_0_do_finalize,
    )
    by_grp_ddf = cast(
        DaskDataFrame,
        source_ddf.groupby(source_ddf.grp)
        .aggregate(
            mean_of_C=("C", "mean"),
            max_of_D=("D", "max"),
        )
    )
    grouped_subgrp_ddf = source_ddf.groupby([source_ddf.grp, source_ddf.subgrp])
    by_subgrp_ddf = cast(
        DaskDataFrame,
        grouped_subgrp_ddf
        .aggregate(
            # var_of_E=("E", "var(ddof=0)"),  # doesn't respect ddof=0
            var_of_E2=("E", custom_var_ddof_0),
        ),
    )
    by_subgrp_ddf = by_subgrp_ddf.assign(
        var_of_E=grouped_subgrp_ddf["E"].var(ddof=0)
    )
    avg_of_vars_ddf = cast(
        DaskDataFrame,
        by_subgrp_ddf
        .reset_index(drop=False)
        .groupby("grp")
        .aggregate(
            avg_var_of_E=("var_of_E", "mean"),
            avg_var_of_E2=("var_of_E2", "mean"),
        )
    )
    merge_1_ddf = (
        pd.merge(
            left=by_grp_ddf.compute(),
            right=avg_of_vars_ddf.compute(),
            on=GROUP_BY_COLUMNS,
            how="inner",
        )
        .reset_index(drop=False)
    )
    merge_2_ddf = (
        merge_1_ddf
        .sort_values(GROUP_BY_COLUMNS)
        .reset_index(drop=True)
    )
    return merge_2_ddf
