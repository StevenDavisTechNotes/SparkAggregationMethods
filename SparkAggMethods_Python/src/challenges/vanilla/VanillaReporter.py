#!python
# python -m VanillaPerfTest.VanillaReporter
import math
import os
from typing import Dict, List, cast

import numpy
import scipy

from challenges.vanilla.VanillaDirectory import (pyspark_implementation_list,
                                                 scala_implementation_list)
from challenges.vanilla.VanillaRunResult import (EXPECTED_SIZES,
                                                 FINAL_REPORT_FILE_PATH,
                                                 SCALA_RUN_LOG_FILE_PATH,
                                                 PersistedRunResult,
                                                 derive_run_log_file_path,
                                                 regressor_from_run_result)
from perf_test_common import (CalcEngine, ExternalTestMethod,
                              TestMethodDescription)
from six_field_test_data.six_generate_test_data_using_pyspark import \
    PysparkPythonTestMethod


def read_python_file(
        engine: CalcEngine,
) -> List[PersistedRunResult]:
    test_runs: List[PersistedRunResult] = []
    with open(derive_run_log_file_path(engine), 'r') as f:
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
                engine=engine,
                strategy_w_language_name=f"{strategy_name}_{language}",
                interface=interface,
                dataSize=int(result_dataSize),
                elapsedTime=float(result_elapsedTime),
                recordCount=int(result_recordCount))
            test_runs.append(result)
    return test_runs


def read_scala_file() -> List[PersistedRunResult]:
    test_runs: List[PersistedRunResult] = []
    with open(SCALA_RUN_LOG_FILE_PATH, 'r') as f:
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
            language = 'scala'
            result = PersistedRunResult(
                strategy_name=strategy_name,
                engine=CalcEngine.SCALA_SPARK,
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
    for engine in [CalcEngine.PYSPARK, CalcEngine.DASK]:
        if os.path.exists(derive_run_log_file_path(engine)):
            cond_runs += read_python_file(engine)
    if os.path.exists(SCALA_RUN_LOG_FILE_PATH):
        cond_runs += read_scala_file()
    return cond_runs


def structure_test_results(
        test_runs: List[PersistedRunResult]
) -> Dict[str, Dict[int, List[PersistedRunResult]]]:
    test_methods = {x.strategy_name for x in pyspark_implementation_list}.union([x.strategy_name for x in test_runs])
    test_x_values = set(EXPECTED_SIZES).union([regressor_from_run_result(x) for x in test_runs])
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


def do_regression(
        python_implementation_list: List[PysparkPythonTestMethod],
        scala_implementation_list: List[ExternalTestMethod],
        test_results: Dict[str, Dict[int, List[PersistedRunResult]]],
) -> str:
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

    for strategy_name in test_results:
        print("Looking to analyze %s" % strategy_name)
        method = [
            x for x in sorted_implementation_list if x.raw_method_name == strategy_name][0]
        for regressor_value in test_results[strategy_name]:
            runs = test_results[strategy_name][regressor_value]
            ar: numpy.ndarray[float, numpy.dtype[numpy.float64]] \
                = numpy.asarray([x.elapsedTime for x in runs], dtype=float)
            numRuns = len(runs)
            mean = numpy.mean(ar)
            stdev = cast(float, numpy.std(ar, ddof=1))
            rl, rh = scipy.stats.norm.interval(  # type: ignore
                confidence, loc=mean, scale=stdev / math.sqrt(numRuns))
            summary_status += "%s,%s,%s,%s,%s,%d,%d,%f,%f,%f,%f\n" % (
                method.data_name,
                method.raw_method_name,
                method.raw_method_name.replace("vanilla_", ""),
                method.language,
                method.interface,
                regressor_value, numRuns, mean, stdev, rl, rh
            )
    return summary_status


def analyze_run_results():
    raw_test_runs = parse_results()
    test_results = structure_test_results(raw_test_runs)
    summary_status = do_regression(
        pyspark_implementation_list,
        scala_implementation_list,
        test_results)

    with open(FINAL_REPORT_FILE_PATH, 'wt') as f:
        f.write(summary_status)
        f.write("\n")


if __name__ == "__main__":
    analyze_run_results()
