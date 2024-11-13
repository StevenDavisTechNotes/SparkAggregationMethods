import datetime as dt

import pandas as pd

from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    Challenge, NumericalToleranceExpectations, SixTestDataSetDescription,
)
from spark_agg_methods_common_python.perf_test_common import RunResultBase


def estimate_expected_summation_error(
        numerical_tolerance: NumericalToleranceExpectations,
        data_description: SixTestDataSetDescription,

) -> float:
    return (
        numerical_tolerance.value
        * max(
            1.0,
            data_description.points_per_index / 1e+5  # originally calibrated at 1e+5
        )
    )


def process_answer(
        challenge: Challenge,
        data_description: SixTestDataSetDescription,
        correct_answer: pd.DataFrame,
        numerical_tolerance: NumericalToleranceExpectations,
        started_time: float,
        df_answer: pd.DataFrame,
        finished_time: float,
) -> RunResultBase:
    if challenge == Challenge.BI_LEVEL:
        if 'avg_var_of_E2' not in df_answer:
            df_answer['avg_var_of_E2'] = df_answer['avg_var_of_E']
    elif challenge == Challenge.CONDITIONAL:
        if 'cond_var_of_E2' not in df_answer:
            df_answer['cond_var_of_E2'] = df_answer['cond_var_of_E']
    elif challenge == Challenge.VANILLA:
        if 'var_of_E2' not in df_answer:
            df_answer['var_of_E2'] = df_answer['var_of_E']
    else:
        raise ValueError("Must return at least 1 type")
    abs_diff = float(
        (correct_answer - df_answer)
        .abs().max().max())
    expected_summation_error = estimate_expected_summation_error(
        numerical_tolerance, data_description)
    if abs_diff >= expected_summation_error:
        raise ValueError(f"abs_diff={abs_diff} >= numerical_tolerance={expected_summation_error}")
    record_count = len(df_answer)
    result = RunResultBase(
        num_source_rows=data_description.num_source_rows,
        elapsed_time=finished_time - started_time,
        num_output_rows=record_count,
        finished_at=dt.datetime.now().isoformat(),
    )
    return result
