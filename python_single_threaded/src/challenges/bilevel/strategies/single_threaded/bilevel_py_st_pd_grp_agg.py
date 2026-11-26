from typing import cast

import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def bilevel_py_st_pd_grp_agg(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"
    df = pd.read_parquet(
        data_set.data.source_file_path_parquet,
        columns=["grp", "subgrp", "C", "D", "E"],
        engine="pyarrow",
    )
    df["E2"] = df["E"] * df["E"]
    by_subgroup = df.groupby(by=["grp", "subgrp"])
    subgroup_df = cast(
        pd.DataFrame,
        pd.DataFrame({
            "var_of_E": by_subgroup["E"].var(ddof=0),
            "var_of_E2": (
                cast(pd.Series, by_subgroup["E2"].mean())
                - cast(pd.Series, by_subgroup["E"].mean()) ** 2
            ),
        })
        .groupby(level="grp")
        .mean(),
    )
    by_group = df.groupby(by="grp")
    result_df = pd.DataFrame({
        "mean_of_C": by_group["C"].mean(),
        "max_of_D": by_group["D"].max(),
    })
    result_df["avg_var_of_E"] = subgroup_df["var_of_E"]
    result_df["avg_var_of_E2"] = subgroup_df["var_of_E2"]
    return result_df.reset_index()
