
import math
from typing import NamedTuple, cast

import numpy
import scipy.stats.norm  # type: ignore

from challenges.bi_level.bi_level_record_runs import (
    EXPECTED_SIZES, FINAL_REPORT_FILE_PATH, PersistedRunResult,
    read_result_file, regressor_from_run_result)
from challenges.bi_level.bi_level_strategy_directory import \
    pyspark_implementation_list
from utils.linear_regression import linear_regression

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


def parse_results() -> list[PersistedRunResult]:
    raw_test_runs: list[PersistedRunResult] = []
    with open(TEMP_RESULT_FILE_PATH, 'w') as fout:
        for result in read_result_file():
            raw_test_runs.append(result)
            fout.write("%s,%s,%d,%d,%f,%d\n" % (result.strategy_name, result.interface,
                       result.dataSize, result.relCard, result.elapsedTime, result.recordCount))
    if len(raw_test_runs) < 1:
        print("no tests")
    return raw_test_runs


def structure_test_results(
        test_runs: list[PersistedRunResult]
) -> dict[str, dict[int, list[PersistedRunResult]]]:
    test_methods = {x.strategy_name for x in pyspark_implementation_list}.union([x.strategy_name for x in test_runs])
    test_x_values = set(EXPECTED_SIZES).union([regressor_from_run_result(x) for x in test_runs])
    test_results = {method: {x: [] for x in test_x_values} for method in test_methods}
    for result in test_runs:
        test_results[result.strategy_name][result.dataSize].append(result)
    return test_results


def make_runs_summary(
        test_results: dict[str, dict[int, list[PersistedRunResult]]]
) -> dict[str, dict[int, int]]:
    return {strategy_name:
            {x_variable: len(runs) for x_variable, runs in runs_for_strategy_name.items()}
            for strategy_name, runs_for_strategy_name in test_results.items()}


def analyze_run_results():
    raw_test_runs = parse_results()
    test_runs_by_startegy_by_size = structure_test_results(raw_test_runs)
    test_runs_summary = make_runs_summary(test_runs_by_startegy_by_size)
    print("test_runs_summary", test_runs_summary)
    if any([num < 10 for details in test_runs_summary.values() for num in details.values()]):
        print("not enough data")
        return
    summary_status = ''
    regression_status = ''
    cond_results: list[PerformanceModelParameters] = []
    confidence = 0.95
    summary_status += "%s,%s,%s,%s,%s,%s,%s,%s\n" % (
        'Method', 'Interface',
        'NumRuns', 'relCard', 'Elapsed Time', 'stdev', 'rl', 'rh'
    )
    regression_status += '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
        'Method', 'Interface',
        'b0_low', 'b0', 'b0_high',
        'b1_low', 'b1', 'b1_high',
        's2_low', 's2', 's2_high')
    for strategy_name in test_runs_by_startegy_by_size:
        print("Looking to analyze %s" % strategy_name)
        cond_method = [
            x for x in pyspark_implementation_list if x.strategy_name == strategy_name][0]
        test_runs_by_size = test_runs_by_startegy_by_size[strategy_name]
        for regressor_value, runs in test_runs_by_size.items():
            ar: numpy.ndarray[float, numpy.dtype[numpy.float64]] \
                = numpy.asarray([x.elapsedTime for x in runs], dtype=float)
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
        x_values = [float(x.relCard) for x in times]
        y_values = [float(x.elapsedTime) for x in times]
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
    analyze_run_results()
