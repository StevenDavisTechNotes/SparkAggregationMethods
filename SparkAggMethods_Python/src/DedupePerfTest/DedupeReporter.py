#!python
# python -m DedupePerfTest.DedupeReporter
import collections
import math
from typing import Dict, List

import numpy
import scipy.stats

from DedupePerfTest.DedupeDirectory import pyspark_implementation_list
from DedupePerfTest.DedupeRunResult import (EXPECTED_NUM_RECORDS,
                                            FINAL_REPORT_FILE_PATH,
                                            PersistedRunResult,
                                            read_result_file,
                                            regressor_from_run_result)
from PerfTestCommon import CalcEngine
from Utils.LinearRegression import linear_regression

TEMP_RESULT_FILE_PATH = "d:/temp/SparkPerfTesting/temp.csv"

TestRegression = collections.namedtuple(
    "TestRegression",
    ["name", "interface", "run_count",
     "b0", "b0_low", "b0_high",
     "b1", "b1_low", "b1_high",
     "s2", "s2_low", "s2_high"])


def structure_test_results(
        test_runs: List[PersistedRunResult]
) -> Dict[str, Dict[int, List[PersistedRunResult]]]:
    test_methods = {x.strategy_name for x in pyspark_implementation_list}.union([x.strategy_name for x in test_runs])
    test_x_values = set(EXPECTED_NUM_RECORDS).union([regressor_from_run_result(x) for x in test_runs])
    test_results = {method: {x: [] for x in test_x_values} for method in test_methods}
    for result in test_runs:
        test_results[result.strategy_name][result.dataSize].append(result)
    return test_results


def make_runs_summary(
        test_results: Dict[str, Dict[int, List[PersistedRunResult]]],
) -> Dict[str, Dict[int, int]]:
    return {strategy_name:
            {x_variable: len(runs) for x_variable, runs in runs_for_strategy_name.items()}
            for strategy_name, runs_for_strategy_name in test_results.items()}


def analyze_run_results(
        engine: CalcEngine,
):
    raw_test_runs: List[PersistedRunResult] = []
    with open(TEMP_RESULT_FILE_PATH, 'w') as fout:
        for result in read_result_file(engine):
            fout.write("%s,%s,%d,%d,%d,%d,%f\n" % (
                result.strategy_name, result.interface,
                result.numSources, result.actualNumPeople,
                result.dataSize, result.dataSizeExp,
                result.elapsedTime))
    if len(raw_test_runs) < 1:
        print("no tests")
    test_results = structure_test_results(raw_test_runs)
    test_runs_summary = make_runs_summary(test_results)
    print("test_runs_summary", test_runs_summary)
    if any([num < 10 for details in test_runs_summary.values() for num in details.values()]):
        print("not enough data")
        return
    summary_status = ''
    regression_status = ''
    test_results = []
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
    for strategy_name in test_results:
        print("Looking to analyze %s" % strategy_name)
        test_method = [
            x for x in pyspark_implementation_list if x.strategy_name == strategy_name][0]
        for regressor_value in test_results[strategy_name]:
            runs = test_results[strategy_name][regressor_value]
            ar = [x.elapsedTime for x in runs]
            mean = numpy.mean(ar)
            stdev = numpy.std(ar, ddof=1)
            rl, rh = scipy.stats.norm.interval(  # type: ignore
                confidence, loc=mean, scale=stdev / math.sqrt(len(ar)))
            summary_status += ("%s,%s," + "%d,%d," + "%f,%f,%f,%f\n") % (
                strategy_name, test_method.interface,
                len(ar), regressor_value,
                mean, stdev, rl, rh
            )
        times = [x for lst in test_results[strategy_name].values() for x in lst]
        x_values = [math.log10(x.dataSize) for x in times]
        y_values = [math.log10(x.elapsedTime) for x in times]
        (b0, (b0_low, b0_high)), (b1, (b1_low, b1_high)), (s2, (s2_low, s2_high)) = \
            linear_regression(x_values, y_values, confidence)
        result = TestRegression(
            name=test_method.strategy_name,
            interface=test_method.interface,
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
            test_method.strategy_name, test_method.interface, result.run_count,
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
