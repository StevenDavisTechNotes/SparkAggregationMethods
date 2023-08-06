
import collections
import math
from typing import Dict, List

import numpy
import scipy

from Utils.LinearRegression import linear_regression

from BiLevelPerfTest.BiLevelDirectory import implementation_list
from BiLevelPerfTest.BiLevelRunResult import (
    EXPECTED_SIZES, FINAL_REPORT_FILE_PATH, PersistedRunResult,
    read_result_file, regressor_from_run_result)

TEMP_RESULT_FILE_PATH = "d:/temp/SparkPerfTesting/temp.csv"

PerformanceModelParameters = collections.namedtuple(
    "PerformanceModelParameters",
    ["name", "interface", "run_count",
     "b0", "b0_low", "b0_high",
     "b1", "b1_low", "b1_high",
     "s2", "s2_low", "s2_high"])


def structure_test_results(
        test_runs: List[PersistedRunResult]
) -> Dict[str, Dict[int, List[PersistedRunResult]]]:
    test_methods = {x.strategy_name for x in implementation_list}.union([x.strategy_name for x in test_runs])
    test_x_values = set(EXPECTED_SIZES).union([regressor_from_run_result(x) for x in test_runs])
    test_results = {method: {x: [] for x in test_x_values} for method in test_methods}
    for result in test_runs:
        test_results[result.strategy_name][result.dataSize].append(result)
    return test_results


def make_runs_summary(test_results: Dict[str, Dict[int, List[PersistedRunResult]]]) -> Dict[str, Dict[int, int]]:
    return {strategy_name:
            {x_variable: len(runs) for x_variable, runs in runs_for_strategy_name.items()}
            for strategy_name, runs_for_strategy_name in test_results.items()}


def analyze_run_results():
    raw_test_runs: List[PersistedRunResult] = []
    with open(TEMP_RESULT_FILE_PATH, 'w') as fout:
        for result in read_result_file():
            raw_test_runs.append(result)
            fout.write("%s,%s,%d,%d,%f,%d\n" % (result.strategy_name, result.interface,
                       result.dataSize, result.relCard, result.elapsedTime, result.recordCount))
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
    cond_results = []
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
    # f.write(("%s,%s,%s,"+"%s,%s,%s,"+"%s,%s,%s,"+"%s,%s,%s\n")%(
    #     'RawMethod', 'interface', 'run_count',
    #     'b0', 'b0 lo', 'b0 hi',
    #     'b1M', 'b1M lo', 'b1M hi',
    #     's2', 's2 lo', 's2 hi'))
    # f.write(("%s,%s,%s,"+"%s,%s,%s,"+"%s,%s\n")% (
    #     'RawMethod', 'interface', 'run_count',
    #     'relCard', 'mean', 'stdev',
    #     'rl', 'rh'
    # ))
    for strategy_name in test_results:
        print("Looking to analyze %s" % strategy_name)
        cond_method = [
            x for x in implementation_list if x.strategy_name == strategy_name][0]
        for regressor_value in test_results[strategy_name]:
            runs = test_results[strategy_name][regressor_value]
            ar = [x.elapsedTime for x in runs]
            numRuns = len(ar)
            mean = numpy.mean(ar)
            stdev = numpy.std(ar, ddof=1)
            rl, rh = scipy.stats.norm.interval(  # type: ignore
                confidence, loc=mean, scale=stdev / math.sqrt(len(ar)))
            # f.write(("%s,%s,"+"%d,%d,"+"%f,%f,%f,%f\n")%(
            #     name, cond_method.interface,
            #     numRuns, relCard,
            #     mean, stdev, rl, rh
            # ))
            summary_status += "%s,%s,%d,%d,%f,%f,%f,%f\n" % (
                strategy_name, cond_method.interface,
                numRuns, regressor_value, mean, stdev, rl, rh
            )
        times = [x for lst in test_results[strategy_name].values() for x in lst]
        x_values = [float(x.relCard) for x in times]
        y_values = [float(x.elapsedTime) for x in times]
        (b0, (b0_low, b0_high)), (b1, (b1_low, b1_high)), (s2, (s2_low, s2_high)) = \
            linear_regression(x_values, y_values, confidence)
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
        # f.write(("%s,%s,%d,"+"%f,%f,%f,"+"%f,%f,%f,"+"%f,%f,%f\n")%(
        #     cond_method.name, cond_method.interface, result.run_count,
        #     result.b0, result.b0_low, result.b0_high,
        #     result.b1*1e+6, result.b1_low*1e+6, result.b1_high*1e+6,
        #     result.s2, result.s2_low, result.s2_high))
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
