#!python
# python -m VanillaPerfTest.VanillaReporter
import math
import os
from typing import cast

import numpy
import scipy

from challenges.vanilla.vanilla_record_runs import (EXPECTED_SIZES,
                                                    FINAL_REPORT_FILE_PATH,
                                                    SCALA_RUN_LOG_FILE_PATH,
                                                    PersistedRunResult,
                                                    derive_run_log_file_path,
                                                    regressor_from_run_result)
from challenges.vanilla.vanilla_strategy_directory import (
    scala_implementation_list, solutions_using_pyspark)
from perf_test_common import (CalcEngine, ChallengeMethodDescription,
                              ChallengeMethodExternalRegistration)
from six_field_test_data.six_generate_test_data import \
    ChallengeMethodPythonPysparkRegistration


def read_python_file(
        engine: CalcEngine,
) -> list[PersistedRunResult]:
    test_runs: list[PersistedRunResult] = []
    with open(derive_run_log_file_path(engine), 'r') as f:
        for line in f:
            line = line.rstrip()
            if line.startswith('#'):
                print("Excluding line: " + line)
                continue
            if line.find(',') < 0:
                print("Excluding line: " + line)
                continue
            fields = line.rstrip().split(',')
            strategy_name, interface, result_data_size, \
                result_elapsed_time, result_record_count, \
                result_datetime, result_blank  \
                = tuple(fields)
            if result_record_count != '9':
                print("Excluding line: " + line)
                continue
            language = 'python'
            result = PersistedRunResult(
                strategy_name=strategy_name,
                language=language,
                engine=engine,
                strategy_w_language_name=f"{strategy_name}_{language}",
                interface=interface,
                data_size=int(result_data_size),
                elapsed_time=float(result_elapsed_time),
                record_count=int(result_record_count))
            test_runs.append(result)
    return test_runs


def read_scala_file() -> list[PersistedRunResult]:
    test_runs: list[PersistedRunResult] = []
    with open(SCALA_RUN_LOG_FILE_PATH, 'r') as f:
        for line in f:
            line = line.rstrip()
            if line.startswith('#'):
                print("Excluding line: " + line)
                continue
            if line.startswith(' '):
                print("Excluding line: " + line)
                continue
            if line.find(',') < 0:
                print("Excluding line: " + line)
                continue
            fields = line.rstrip().split(',')
            outcome, strategy_name, interface, expected_size, returnedSize, elapsed_time = tuple(
                fields)
            if outcome != 'success':
                print("Excluding line: " + line)
                continue
            if returnedSize != '9':
                print("Excluding line: " + line)
                continue
            language = 'scala'
            result = PersistedRunResult(
                strategy_name=strategy_name,
                engine=CalcEngine.SCALA_SPARK,
                language=language,
                strategy_w_language_name=f"{strategy_name}_{language}",
                interface=interface,
                data_size=int(expected_size),
                elapsed_time=float(elapsed_time),
                record_count=-1)
            test_runs.append(result)
    return test_runs


def parse_results() -> list[PersistedRunResult]:
    cond_runs: list[PersistedRunResult] = []
    for engine in [CalcEngine.PYSPARK, CalcEngine.DASK]:
        if os.path.exists(derive_run_log_file_path(engine)):
            cond_runs += read_python_file(engine)
    if os.path.exists(SCALA_RUN_LOG_FILE_PATH):
        cond_runs += read_scala_file()
    return cond_runs


def structure_test_results(
        test_runs: list[PersistedRunResult]
) -> dict[str, dict[int, list[PersistedRunResult]]]:
    challenge_method_registrations = (
        {x.strategy_name for x in solutions_using_pyspark}
        .union([x.strategy_name for x in test_runs]))
    test_x_values = set(EXPECTED_SIZES).union([regressor_from_run_result(x) for x in test_runs])
    test_results = {method: {x: [] for x in test_x_values} for method in challenge_method_registrations}
    for result in test_runs:
        test_results[result.strategy_name][result.data_size].append(result)
    return test_results


def make_runs_summary(
        test_results: dict[str, dict[int, list[PersistedRunResult]]],
) -> dict[str, dict[int, int]]:
    return {strategy_name:
            {x_variable: len(runs) for x_variable, runs in runs_for_strategy_name.items()}
            for strategy_name, runs_for_strategy_name in test_results.items()}


def do_regression(
        python_implementation_list: list[ChallengeMethodPythonPysparkRegistration],
        scala_implementation_list: list[ChallengeMethodExternalRegistration],
        test_results: dict[str, dict[int, list[PersistedRunResult]]],
) -> str:
    summary_status = ''
    confidence = 0.95
    sorted_implementation_list = sorted(
        [ChallengeMethodDescription(
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
                = numpy.asarray([x.elapsed_time for x in runs], dtype=float)
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
        solutions_using_pyspark,
        scala_implementation_list,
        test_results)

    with open(FINAL_REPORT_FILE_PATH, 'wt') as f:
        f.write(summary_status)
        f.write("\n")


if __name__ == "__main__":
    analyze_run_results()
