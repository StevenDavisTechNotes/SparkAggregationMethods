from typing import cast

from dask.dataframe.core import DataFrame as DaskDataFrame
from dask_sql import Context

from src.six_field_test_data.six_generate_test_data import DataSetDask, TChallengeAnswerPythonDask
from src.six_field_test_data.six_test_data_types import SixTestExecutionParameters

# pyright: reportArgumentType=false
# pyright: reportCallIssue=false


def bi_level_dask_sql_nested_no_gpu(
        exec_params: SixTestExecutionParameters,
        data_set: DataSetDask,
) -> TChallengeAnswerPythonDask:
    df = data_set.data.df_src
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
                grp,
                SUM(sub_sum_of_C) / SUM(sub_count) as mean_of_C,
                MAX(sub_max_of_D) as max_of_D,
                AVG(sub_var_of_E) as avg_var_of_E,
                AVG(
                    sub_sum_of_E_squared / sub_count -
                    POWER(sub_sum_of_E / sub_count, 2)
                ) as avg_var_of_E2
        FROM
            (SELECT
                    grp, subgrp,
                    count(C) as sub_count,
                    sum(C) as sub_sum_of_C,
                    max(D) as sub_max_of_D,
                    VAR_POP(E) as sub_var_of_E,
                    sum(E * E) as sub_sum_of_E_squared,
                    sum(E) as sub_sum_of_E
                FROM
                    example_data
                GROUP BY grp, subgrp) level2
        GROUP BY grp
        ORDER BY grp
    """,
        gpu=gpu,
    )
    df = cast(DaskDataFrame, result.compute())
    df = cast(DaskDataFrame,
              df.rename(columns={
                  "mean_of_c": "mean_of_C",
                  "max_of_d": "max_of_D",
                  "avg_var_of_e": "avg_var_of_E",
                  "avg_var_of_e2": "avg_var_of_E2",
              }))
    return df
