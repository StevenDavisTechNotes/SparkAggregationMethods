import numpy
import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def vanilla_py_st_pd_grp_numpy(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"
    df = pd.read_parquet(
        data_set.data.source_file_path_parquet,
        engine="pyarrow",
    )
    df_result = (
        df
        .groupby(by=["grp", "subgrp"])
        .apply(inner_agg_method)
        .sort_values(by=["grp", "subgrp"])  # pyright: ignore[reportCallIssue]
        .reset_index(drop=False)
    )
    return df_result


def inner_agg_method(
        partition_df: pd.DataFrame,
) -> pd.Series:
    c = partition_df["C"]
    d = partition_df["D"]
    e = partition_df["E"]
    return pd.Series({
        "mean_of_C": numpy.mean(c),
        "max_of_D": numpy.max(d),
        "var_of_E": numpy.var(e),
        "var_of_E2": (
            numpy.inner(e, e) / e.count()
            - (numpy.sum(e) / e.count())**2
        ),
    }, dtype=float)
