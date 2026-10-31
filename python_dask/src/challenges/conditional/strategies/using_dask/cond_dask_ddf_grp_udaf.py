from typing import cast

import numpy
import pandas as pd
from dask.dataframe import DataFrame as DaskDataFrame
from dask.dataframe import groupby
from pandas.core.groupby.generic import SeriesGroupBy
from spark_agg_methods_common_python.challenges.conditional.conditional_test_data_types import GROUP_BY_COLUMNS
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


def cond_dask_ddf_grp_udaf(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask
) -> TChallengeAnswerPythonDask:
    if (data_set.data_description.points_per_index > 10**4):
        return "infeasible", "Unknown reason"
    source_ddf: DaskDataFrame = data_set.data.open_source_data_as_ddf()
    col_e = source_ddf["E"]
    w_neg_e_ddf = (
        source_ddf
        .assign(negE=col_e.where(col_e < 0))
    )
    custom_var_ddof_0 = groupby.Aggregation(
        name="custom_var_ddof_0",
        chunk=ddof_0_do_chunk,
        agg=ddof_0_do_agg,
        finalize=ddof_0_do_finalize,
    )
    grouped_ddf = w_neg_e_ddf.groupby([w_neg_e_ddf["grp"], w_neg_e_ddf["subgrp"]])
    most_cols_df: pd.DataFrame = cast(
        DaskDataFrame,
        grouped_ddf
        .aggregate(
            mean_of_C=("C", "mean"),
            max_of_D=("D", "max"),
            # cond_var_of_E=("negE", "var(ddof=0)"),  # doesn't respect ddof=0
            cond_var_of_E2=("negE", custom_var_ddof_0),
        ),
    ).compute()
    just_var_of_e_df: pd.DataFrame = (
        grouped_ddf["negE"]
        .var(ddof=0)
        .to_frame("cond_var_of_E")
        .compute()
    )
    result_df = (
        pd.merge(
            left=most_cols_df,
            right=just_var_of_e_df,
            on=GROUP_BY_COLUMNS,
            how="inner",
        )
        .reset_index(drop=False)
        .sort_values(GROUP_BY_COLUMNS)
        .reset_index(drop=True)
    )
    return result_df
