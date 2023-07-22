#!python
# python -m VanillaPerfTest.VanillaReporter
import math
import os
from typing import Dict, List

import numpy
from scipy.stats import norm as scipy_stats_norm  # type: ignore

from PerfTestCommon import ExternalTestMethod, TestMethodDescription
from SixFieldCommon.SixFieldTestData import PythonTestMethod

from .VanillaDirectory import implementation_list, scala_implementation_list
from .VanillaRunResult import (EXPECTED_SIZES, FINAL_REPORT_FILE_PATH, PYTHON_RESULT_FILE_PATH,
                               SCALA_RESULT_FILE_PATH, PersistedRunResult, RunResult, regressor_from_run_result)


def read_python_file() -> List[PersistedRunResult]:
    test_runs: List[PersistedRunResult] = []
    with open(PYTHON_RESULT_FILE_PATH, 'r') as f:
        for textline in f:
            textline = textline.rstrip()
            if textline.startswith('#'):
                print("Excluding line: " + textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: " + textline)
                continue
            fields = textline.rstrip().split(',')
            strategy_name, interface, result_dataSize, \
                result_elapsedTime, result_recordCount, \
                result_datetime, result_blank  \
                = tuple(fields)
            if result_recordCount != '9':
                print("Excluding line: " + textline)
                continue
            language = 'python'
            result = PersistedRunResult(
                strategy_name=strategy_name,
                language=language,
                strategy_w_language_name=f"{strategy_name}_{language}",
                interface=interface,
                dataSize=int(result_dataSize),
                elapsedTime=float(result_elapsedTime),
                recordCount=int(result_recordCount))
            test_runs.append(result)
    return test_runs


def read_scala_file() -> List[PersistedRunResult]:
    test_runs: List[PersistedRunResult] = []
    with open(SCALA_RESULT_FILE_PATH, 'r') as f:
        for textline in f:
            textline = textline.rstrip()
            if textline.startswith('#'):
                print("Excluding line: " + textline)
                continue
            if textline.startswith(' '):
                print("Excluding line: " + textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: " + textline)
                continue
            fields = textline.rstrip().split(',')
            outcome, strategy_name, interface, expectedSize, returnedSize, elapsedTime = tuple(
                fields)
            if outcome != 'success':
                print("Excluding line: " + textline)
                continue
            if returnedSize != '9':
                print("Excluding line: " + textline)
                continue
            language='scala'
            result = PersistedRunResult(
                strategy_name=strategy_name,
                language=language,
                strategy_w_language_name=f"{strategy_name}_{language}",
                interface=interface,
                dataSize=int(expectedSize),
                elapsedTime=float(elapsedTime),
                recordCount=-1)
            test_runs.append(result)
    return test_runs


def parse_results() -> List[PersistedRunResult]:
    cond_runs: List[PersistedRunResult] = []
    if os.path.exists(PYTHON_RESULT_FILE_PATH):
        cond_runs += read_python_file()
    if os.path.exists(SCALA_RESULT_FILE_PATH):
        cond_runs += read_scala_file()
    return cond_runs


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



def do_regression(python_implementation_list: List[PythonTestMethod],
                  scala_implementation_list: List[ExternalTestMethod], cond_runs):
    summary_status = ''
    confidence = 0.95
    sorted_implementation_list = sorted(
        [TestMethodDescription(
            data_name=f"{x.strategy_name}_{x.language}", raw_method_name=x.strategy_name,
            language=x.language, interface=x.interface)
            for x in python_implementation_list + scala_implementation_list],
        key=lambda x: (x.language, x.interface, x.raw_method_name))
    summary_status += ",".join([
        'RunName', 'RawMethod', 'Method', 'Language', 'Interface',
        'DataSize', 'NumRuns', 'Elapsed Time', 'stdev', 'rl', 'rh']) + "\n"
    for method in sorted_implementation_list:
        times = cond_runs[method.data_name] if method.data_name in cond_runs else [
        ]
        size_values = set(x.dataSize for x in times)
        for dataSize in sorted(size_values):
            ar = [x.elapsedTime for x in times if x.dataSize == dataSize]
            numRuns = len(ar)
            mean = numpy.mean(ar)
            stdev = numpy.std(ar, ddof=1)
            rl, rh = scipy_stats_norm.interval(
                confidence, loc=mean, scale=stdev / math.sqrt(len(ar)))
            summary_status += "%s,%s,%s,%s,%s,%d,%d,%f,%f,%f,%f\n" % (
                method.data_name,
                method.raw_method_name,
                method.raw_method_name.replace("vanilla_", ""),
                method.language,
                method.interface,
                dataSize, numRuns, mean, stdev, rl, rh
            )
    return summary_status


def analyze_run_results():
    cond_runs = parse_results()
    summary_status = do_regression(
        implementation_list,
        scala_implementation_list,
        cond_runs)

    with open(FINAL_REPORT_FILE_PATH, 'wt') as f:
        f.write(summary_status)
        f.write("\n")


if __name__ == "__main__":
    analyze_run_results()
