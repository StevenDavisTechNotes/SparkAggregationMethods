from typing import cast

from dask.dataframe.core import DataFrame as DaskDataFrame
from dask_sql import Context
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask

# pyright: reportArgumentType=false
# pyright: reportCallIssue=false


def vanilla_dask_sql_no_gpu(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask,
) -> TChallengeAnswerPythonDask:
    if (data_set.data_description.points_per_index >= 10**6):  # incorrect answer at that point
        return "infeasible"
    df = data_set.data.open_source_data_as_ddf()
    return inner(df, gpu=False)


def inner(df: DaskDataFrame, gpu: bool) -> DaskDataFrame:
    c = Context()
    df = cast(DaskDataFrame,
              df.rename(columns={
                  "A": "a",
                  "B": "b",
                  "C": "c",
                  "D": "d",
                  "E": "e",
                  "F": "f",
              }))
    c.create_table(
        gpu=gpu,
        input_table=df,
        persist=False,
        table_name="example_data",
    )
    result = c.sql(
        sql="""
        SELECT
            grp, subgrp, AVG(C) mean_of_C, MAX(D) max_of_D,
            VAR_POP(E) var_of_E,
            (
                SUM(E * E) / COUNT(E) -
                POWER(SUM(E) / COUNT(E), 2)
            ) var_of_E2
        FROM
            example_data
        GROUP BY grp, subgrp
        ORDER BY grp, subgrp
    """,
        gpu=gpu,
    )
    df = cast(DaskDataFrame, result.compute())
    df = cast(DaskDataFrame,
              df.rename(columns={
                  "mean_of_c": "mean_of_C",
                  "max_of_d": "max_of_D",
                  "var_of_e": "var_of_E",
                  "var_of_e2": "var_of_E2",
              }))
    return df
