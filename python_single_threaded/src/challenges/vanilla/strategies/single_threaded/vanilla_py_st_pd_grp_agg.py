from typing import cast

import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)
from src.challenges.six_field_test_data.six_test_data_for_py_st import SixDataSetPythonST, TChallengePythonSTAnswer


def vanilla_py_st_pd_grp_agg(
        exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonST,
) -> TChallengePythonSTAnswer:
    if data_set.data_description.num_source_rows > 9 * 10**6:
        return "infeasible", "Unknown reason"
    source_df = pd.read_parquet(
        data_set.data.source_file_path_parquet,
        columns=["grp", "subgrp", "C", "D", "E"],
        engine="pyarrow",
    )
    source_df["E2"] = source_df["E"] * source_df["E"]
    grouped = source_df.groupby(by=["grp", "subgrp"])
    result_df = (
        pd.DataFrame({
            "mean_of_C": grouped["C"].mean(),
            "max_of_D": grouped["D"].max(),
            "var_of_E": grouped["E"].var(ddof=0),
            "var_of_E2": (
                cast(pd.Series, grouped["E2"].mean())
                - cast(pd.Series, grouped["E"].mean()) ** 2
            ),
        })
        .reset_index()
    )
    return result_df
