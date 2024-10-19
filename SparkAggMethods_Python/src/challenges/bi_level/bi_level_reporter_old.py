#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.challenges.bi_level.bi_level_reporter_old

import math
from typing import NamedTuple, cast

import numpy
import scipy
from spark_agg_methods_common_python.perf_test_common import CalcEngine, print_test_runs_summary

from src.challenges.bi_level.bi_level_record_runs import (
    EXPECTED_SIZES, FINAL_REPORT_FILE_PATH, BiLevelPersistedRunResult, BiLevelPersistedRunResultLog,
    regressor_from_run_result,
)
from src.challenges.bi_level.bi_level_strategy_directory import STRATEGIES_USING_PYSPARK_REGISTRY
from src.utils.linear_regression import linear_regression

TEMP_RESULT_FILE_PATH = "d:/temp/SparkPerfTesting/temp.csv"


class PerformanceModelParameters(NamedTuple):
    name: str
    interface: str
    run_count: int
    b0: float
    b0_low: float
    b0_high: float
    b1: float
    b1_low: float
    b1_high: float
    s2: float
    s2_low: float
    s2_high: float


def parse_results(calc_engine: CalcEngine) -> list[BiLevelPersistedRunResult]:
    reader = BiLevelPersistedRunResultLog(calc_engine)
    raw_test_runs = cast(
        list[BiLevelPersistedRunResult],
        reader.read_run_result_file())
    with open(TEMP_RESULT_FILE_PATH, 'w') as out_fh:
        for result in raw_test_runs:
            out_fh.write("%s,%s,%d,%d,%f,%d\n" % (
                result.strategy_name,
                result.interface,
                result.num_source_rows,
                result.relative_cardinality_between_groupings,
                result.elapsed_time,
                result.num_output_rows,
            ))
    if len(raw_test_runs) < 1:
        print("no tests")
    return raw_test_runs


def structure_test_results(
        test_runs: list[BiLevelPersistedRunResult]
) -> dict[str, dict[int, list[BiLevelPersistedRunResult]]]:
    challenge_method_registrations = {x.strategy_name for x in STRATEGIES_USING_PYSPARK_REGISTRY}.union(
        [x.strategy_name for x in test_runs])
    test_x_values = set(EXPECTED_SIZES).union([regressor_from_run_result(x) for x in test_runs])
    test_results = {method: {x: [] for x in test_x_values} for method in challenge_method_registrations}
    for result in test_runs:
        test_results[result.strategy_name][regressor_from_run_result(result)].append(result)
    return test_results


def make_runs_summary(
        test_results: dict[str, dict[int, list[BiLevelPersistedRunResult]]]
) -> dict[str, dict[int, int]]:
    return {strategy_name:
            {x_variable: len(runs) for x_variable, runs in runs_for_strategy_name.items()}
            for strategy_name, runs_for_strategy_name in test_results.items()}


def analyze_run_results():
    raw_test_runs = parse_results(CalcEngine.PYSPARK)
    test_runs_by_strategy_by_size = structure_test_results(raw_test_runs)
    test_runs_summary = make_runs_summary(test_runs_by_strategy_by_size)
    print_test_runs_summary(test_runs_summary)
    if any([num < 10 for details in test_runs_summary.values() for num in details.values()]):
        print("not enough data")
        return
    summary_status = ''
    regression_status = ''
    cond_results: list[PerformanceModelParameters] = []
    confidence = 0.95
    summary_status += "%s,%s,%s,%s,%s,%s,%s,%s\n" % (
        'Method', 'Interface',
        'NumRuns', 'Relative Cardinality', 'Elapsed Time', 'stdev', 'rl', 'rh'
    )
    regression_status += '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
        'Method', 'Interface',
        'b0_low', 'b0', 'b0_high',
        'b1_low', 'b1', 'b1_high',
        's2_low', 's2', 's2_high')
    for strategy_name in test_runs_by_strategy_by_size:
        print("Looking to analyze %s" % strategy_name)
        cond_method = [
            x for x in STRATEGIES_USING_PYSPARK_REGISTRY if x.strategy_name == strategy_name][0]
        test_runs_by_size = test_runs_by_strategy_by_size[strategy_name]
        for regressor_value, runs in test_runs_by_size.items():
            ar: numpy.ndarray[float, numpy.dtype[numpy.float64]] \
                = numpy.asarray([x.elapsed_time for x in runs], dtype=float)
            numRuns = len(runs)
            mean = numpy.mean(ar)
            stdev = cast(float, numpy.std(ar, ddof=1))
            rl, rh = scipy.stats.norm.interval(
                confidence, loc=mean, scale=stdev / math.sqrt(numRuns))
            summary_status += "%s,%s,%d,%d,%f,%f,%f,%f\n" % (
                strategy_name, cond_method.interface,
                numRuns, regressor_value, mean, stdev, rl, rh
            )
        times = [x for lst in test_runs_by_size.values() for x in lst]
        x_values = [float(x.relative_cardinality_between_groupings) for x in times]
        y_values = [float(x.elapsed_time) for x in times]
        match linear_regression(x_values, y_values, confidence):
            case None:
                print("No regression")
                return
            case (b0, (b0_low, b0_high)), (b1, (b1_low, b1_high)), (s2, (s2_low, s2_high)):
                pass
        result = PerformanceModelParameters(
            name=cond_method.strategy_name,
            interface=cond_method.interface,
            run_count=len(times),
            b0=b0,
            b0_low=b0_low,
            b0_high=b0_high,
            b1=b1,
            b1_low=b1_low,
            b1_high=b1_high,
            s2=s2,
            s2_low=s2_low,
            s2_high=s2_high
        )
        cond_results.append(result)
        regression_status += '%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f\n' % (
            cond_method.strategy_name, cond_method.interface,
            result.b0_low, result.b0, result.b0_high,
            result.b1_low, result.b1, result.b1_high,
            result.s2_low, result.s2, result.s2_high)

    with open(FINAL_REPORT_FILE_PATH, 'w') as f:
        f.write(summary_status)
        f.write("\n")
        f.write(regression_status)
        f.write("\n")


if __name__ == "__main__":
    print(f"Running {__file__}")
    analyze_run_results()
    print("Done!")
