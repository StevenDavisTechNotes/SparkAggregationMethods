#!python
# python -m DedupePerfTest.DedupeReporter
import math
from typing import NamedTuple, cast

import numpy
import scipy.stats

from challenges.deduplication.dedupe_record_runs import (
    EXPECTED_NUM_RECORDS, FINAL_REPORT_FILE_PATH, PersistedRunResult,
    read_result_file, regressor_from_run_result)
from challenges.deduplication.dedupe_strategy_directory import \
    solutions_using_pyspark
from perf_test_common import CalcEngine
from utils.linear_regression import linear_regression

TEMP_RESULT_FILE_PATH = "d:/temp/SparkPerfTesting/temp.csv"


class TestRegression(NamedTuple):
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


def structure_test_results(
        test_runs: list[PersistedRunResult]
) -> dict[str, dict[int, list[PersistedRunResult]]]:
    challenge_method_registrations = (
        {x.strategy_name for x in solutions_using_pyspark}
        .union([x.strategy_name for x in test_runs]))
    test_x_values = set(EXPECTED_NUM_RECORDS).union([regressor_from_run_result(x) for x in test_runs])
    test_results: dict[str, dict[int, list[PersistedRunResult]]] \
        = {
            method: {
                x: []
                for x in test_x_values}
        for method in challenge_method_registrations}
    for result in test_runs:
        test_results[result.strategy_name][result.dataSize] \
            .append(result)
    return test_results


def make_runs_summary(
        test_results: dict[str, dict[int, list[PersistedRunResult]]],
) -> dict[str, dict[int, int]]:
    return {
        strategy_name:
            {x_variable: len(runs) for x_variable, runs in runs_for_strategy_name.items()}
            for strategy_name, runs_for_strategy_name in test_results.items()
    }


def analyze_run_results(
        engine: CalcEngine,
):
    raw_test_runs: list[PersistedRunResult] = []
    with open(TEMP_RESULT_FILE_PATH, 'w') as out_fh:
        for result in read_result_file(engine):
            out_fh.write("%s,%s,%d,%d,%d,%d,%f\n" % (
                result.strategy_name, result.interface,
                result.numSources, result.actualNumPeople,
                result.dataSize, result.dataSizeExp,
                result.elapsedTime))
            raw_test_runs.append(result)
    if len(raw_test_runs) < 1:
        print("no tests")
    test_runs_by_strategy_by_size: dict[str, dict[int, list[PersistedRunResult]]
                                        ] = structure_test_results(raw_test_runs)
    test_runs_summary = make_runs_summary(test_runs_by_strategy_by_size)
    print("test_runs_summary", test_runs_summary)
    if any([num < 10 for details in test_runs_summary.values() for num in details.values()]):
        print("not enough data")
        return
    summary_status = ''
    regression_status = ''
    test_results: list[TestRegression] = []
    confidence = 0.95
    regression_status += ("%s,%s,%s," + "%s,%s,%s," + "%s,%s,%s," + "%s,%s,%s\n") % (
        'RawMethod', 'interface', 'run_count',
        'b0', 'b0 lo', 'b0 hi',
        'b1M', 'b1M lo', 'b1M hi',
        's2', 's2 lo', 's2 hi')
    summary_status += ("%s,%s,%s," + "%s,%s,%s," + "%s,%s\n") % (
        'RawMethod', 'interface', 'run_count',
        'DataSize', 'mean', 'stdev',
        'rl', 'rh')
    for strategy_name in test_runs_by_strategy_by_size:
        print("Looking to analyze %s" % strategy_name)
        challenge_method_registrations = [
            x for x in solutions_using_pyspark if x.strategy_name == strategy_name][0]
        test_runs_by_size = test_runs_by_strategy_by_size[strategy_name]
        for regressor_value, runs in test_runs_by_size.items():
            ar: numpy.ndarray[float, numpy.dtype[numpy.float64]] \
                = numpy.asarray([x.elapsedTime for x in runs], dtype=float)
            numRuns = len(runs)
            mean = numpy.mean(ar)
            stdev = cast(float, numpy.std(ar, ddof=1))
            rl, rh = scipy.stats.norm.interval(  # type: ignore
                confidence, loc=mean, scale=stdev / math.sqrt(numRuns))
            summary_status += ("%s,%s," + "%d,%d," + "%f,%f,%f,%f\n") % (
                strategy_name, challenge_method_registrations.interface,
                len(ar), regressor_value,
                mean, stdev, rl, rh
            )
        times = [x for lst in test_runs_by_size.values() for x in lst]
        x_values = [math.log10(x.dataSize) for x in times]
        y_values = [math.log10(x.elapsedTime) for x in times]
        match linear_regression(x_values, y_values, confidence):
            case None:
                print("No regression")
                return
            case (b0, (b0_low, b0_high)), (b1, (b1_low, b1_high)), (s2, (s2_low, s2_high)):
                pass
        result = TestRegression(
            name=challenge_method_registrations.strategy_name,
            interface=challenge_method_registrations.interface,
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
        test_results.append(result)
        regression_status += ("%s,%s,%d," + "%f,%f,%f," + "%f,%f,%f," + "%f,%f,%f\n") % (
            challenge_method_registrations.strategy_name, challenge_method_registrations.interface, result.run_count,
            result.b0, result.b0_low, result.b0_high,
            result.b1 * 1e+6, result.b1_low * 1e+6, result.b1_high * 1e+6,
            result.s2, result.s2_low, result.s2_high)
    with open(FINAL_REPORT_FILE_PATH, 'w') as f:
        f.write(summary_status)
        f.write("\n")
        f.write(regression_status)
        f.write("\n")


if __name__ == "__main__":
    analyze_run_results(CalcEngine.PYSPARK)
    analyze_run_results(CalcEngine.DASK)
