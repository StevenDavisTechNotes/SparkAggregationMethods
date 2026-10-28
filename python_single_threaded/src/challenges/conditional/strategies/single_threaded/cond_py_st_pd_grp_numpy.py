import math

import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def cond_py_st_pd_grp_numpy(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"
    df = pd.read_parquet(data_set.data.source_file_path_parquet, engine="pyarrow")
    df_result = (
        df
        .groupby(by=["grp", "subgrp"])
        .apply(inner_agg_method)
        .sort_values(by=["grp", "subgrp"])  # pyright: ignore[reportCallIssue]
        .reset_index(drop=False)
    )
    return df_result


def my_loop_lift_var(
        x: pd.Series,
) -> float:
    n = x.count()
    return numpy.inner(x, x) / n - (numpy.sum(x) / n)**2 if n > 0 else math.nan


def inner_agg_method(
        dfPartition: pd.DataFrame,
) -> pd.Series:
    C = dfPartition["C"]
    D = dfPartition["D"]
    negE = dfPartition.loc[dfPartition["E"] < 0, "E"]
    return pd.Series({
        "mean_of_C": numpy.mean(C),
        "max_of_D": numpy.max(D),
        "cond_var_of_E": numpy.var(negE),
        "cond_var_of_E2": my_loop_lift_var(negE),
    }, dtype=float)
