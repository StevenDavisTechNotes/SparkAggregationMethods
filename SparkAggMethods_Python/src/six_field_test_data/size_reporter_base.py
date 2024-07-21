import math
from typing import Callable, Sequence

import numpy
import pandas as pd
import scipy

from src.challenges.vanilla.vanilla_record_runs import PersistedRunResult
from src.perf_test_common import ChallengeMethodRegistration
from src.six_field_test_data.six_runner_base import \
    SummarizedPerformanceOfMethodAtDataSize
from src.six_field_test_data.six_test_data_types import Challenge


def structure_test_results(
        *,
        challenge_method_list: Sequence[ChallengeMethodRegistration],
        expected_sizes: list[int],
        test_runs: list[PersistedRunResult],
        regressor_from_run_result: Callable[[PersistedRunResult], int],
) -> dict[str, dict[int, list[PersistedRunResult]]]:
    strategy_names = (
        {x.strategy_name for x in challenge_method_list}
        .union([x.strategy_name for x in test_runs]))
    test_x_values = set(expected_sizes).union([regressor_from_run_result(x) for x in test_runs])
    test_results_by_strategy_name_by_data_size = {method: {x: [] for x in test_x_values} for method in strategy_names}
    for result in test_runs:
        test_results_by_strategy_name_by_data_size[result.strategy_name][result.data_size].append(result)
    return test_results_by_strategy_name_by_data_size


def do_regression(
        challenge: Challenge,
        challenge_method_list: Sequence[ChallengeMethodRegistration],
        test_results_by_strategy_name_by_data_size: dict[str, dict[int, list[PersistedRunResult]]],
) -> list[SummarizedPerformanceOfMethodAtDataSize]:
    confidence = 0.95
    challenge_method_list = sorted(
        challenge_method_list,
        key=lambda x: (x.language, x.interface, x.strategy_name))
    challenge_method_by_strategy_name = ({
        x.strategy_name: x for x in challenge_method_list
    } | {
        x.strategy_name_2018: x for x in challenge_method_list
        if x.strategy_name_2018 is not None
    })

    rows: list[SummarizedPerformanceOfMethodAtDataSize] = []
    for strategy_name in test_results_by_strategy_name_by_data_size:
        print("Looking to analyze %s" % strategy_name)
        method = challenge_method_by_strategy_name[strategy_name]
        for regressor_value in test_results_by_strategy_name_by_data_size[strategy_name]:
            runs = test_results_by_strategy_name_by_data_size[strategy_name][regressor_value]
            ar: numpy.ndarray[float, numpy.dtype[numpy.float64]] \
                = numpy.asarray([x.elapsed_time for x in runs], dtype=float)
            numRuns = len(runs)
            mean = numpy.mean(ar).item()
            stdev = numpy.std(ar, ddof=1).item()
            rl, rh = (
                scipy.stats.norm.interval(
                    confidence, loc=mean, scale=stdev / math.sqrt(numRuns))
                if numRuns > 1 else
                (math.nan, math.nan)
            )
            rows.append(SummarizedPerformanceOfMethodAtDataSize(
                challenge=challenge,
                strategy_name=strategy_name,
                language=method.language,
                interface=method.interface,
                regressor=regressor_value,
                number_of_runs=numRuns,
                elapsed_time_avg=mean,
                elapsed_time_std=stdev,
                elapsed_time_rl=rl,
                elapsed_time_rh=rh,
            ))
    return rows


def print_summary(
    rows: list[SummarizedPerformanceOfMethodAtDataSize],
    file_report_file_path: str,
):
    df = pd.DataFrame([x.__dict__ for x in rows])
    df = df.sort_values(by=['challenge', 'strategy_name', 'language', 'regressor'])
    print(df)
    df.to_csv(file_report_file_path, index=False)
