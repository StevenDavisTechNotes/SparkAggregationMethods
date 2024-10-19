#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.challenges.sectional.section_reporter
import math
import os
from typing import NamedTuple, cast

import numpy
import scipy.stats
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, Challenge, SolutionLanguage, SummarizedPerformanceOfMethodAtDataSize, parse_interface_python,
)

from src.challenges.sectional.section_record_runs import (
    FINAL_REPORT_FILE_PATH, SectionPersistedRunResult, SectionPythonPersistedRunResultLog, derive_run_log_file_path,
    regressor_from_run_result,
)
from src.challenges.sectional.section_strategy_directory import (
    STRATEGIES_USING_DASK_REGISTRY, STRATEGIES_USING_PYSPARK_REGISTRY, STRATEGIES_USING_PYTHON_ONLY_REGISTRY,
)
from src.utils.linear_regression import linear_regression

LANGUAGE = SolutionLanguage.PYTHON
CHALLENGE = Challenge.SECTIONAL
EXPECTED_SIZES = [
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000
]


class TestRegression(NamedTuple):
    name: str
    interface: str
    scale: str
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


def analyze_run_results_old():
    test_runs = read_run_results_old()
    if len(test_runs) < 1:
        print("no tests")
        return

    summary_status = ''
    regression_status = ''
    test_results = []
    confidence = 0.95
    regression_status += ("%s,%s,%s,%s," + "%s,%s,%s," + "%s,%s,%s," + "%s,%s,%s\n") % (
        'RawMethod', 'interface', 'scale', 'run_count',
        'b0', 'b0 lo', 'b0 hi',
        'b1M', 'b1M lo', 'b1M hi',
        's2', 's2 lo', 's2 hi')
    summary_status += ("%s,%s,%s,%s," + "%s,%s,%s," + "%s,%s\n") % (
        'RawMethod', 'interface', 'scale', 'run_count',
        'SectionMaximum', 'mean', 'stdev',
        'rl', 'rh')

    test_strategy_names = sorted({x.strategy_name for x in test_runs})
    for strategy_name in test_strategy_names:
        results_for_strategy = [x for x in test_runs if x.strategy_name == strategy_name]
        test_engines = {x.engine for x in results_for_strategy}
        for engine in test_engines:
            times = [x for x in results_for_strategy if x.engine == engine]
            if len(times) == 0:
                continue
            if len(times) < 10:
                print(f"not enough data {test_strategy_names}/{engine.value}")
                return
    for strategy_name in test_strategy_names:
        results_for_strategy = [x for x in test_runs if x.strategy_name == strategy_name]
        test_engines = {x.engine for x in results_for_strategy}
        for engine in test_engines:
            times = [x for x in results_for_strategy if x.engine == engine]
            if len(times) == 0:
                continue
            challenge_method_registration = (
                [x for x in STRATEGIES_USING_DASK_REGISTRY if x.strategy_name == strategy_name]
                + [x for x in STRATEGIES_USING_PYSPARK_REGISTRY if x.strategy_name == strategy_name]
                + [x for x in STRATEGIES_USING_PYTHON_ONLY_REGISTRY if x.strategy_name == strategy_name])[0]

            size_values = set(x.num_source_rows for x in times)
            for num_rows in size_values:
                runs = [x for x in times if x.num_source_rows == num_rows]
                ar: numpy.ndarray[float, numpy.dtype[numpy.float64]] \
                    = numpy.asarray([x.elapsed_time for x in runs], dtype=float)
                numRuns = len(runs)
                mean = numpy.mean(ar)
                stdev = cast(float, numpy.std(ar, ddof=1))
                rl, rh = scipy.stats.norm.interval(  # type: ignore
                    confidence, loc=mean, scale=stdev / math.sqrt(numRuns))
                summary_status += ("%s,%s,%s," + "%d,%d," + "%f,%f,%f,%f\n") % (
                    challenge_method_registration.strategy_name,
                    challenge_method_registration.interface,
                    challenge_method_registration.scale,
                    len(ar), num_rows,
                    mean, stdev, rl, rh
                )
            x_values = [float(x.num_source_rows) for x in times]
            y_values = [x.elapsed_time for x in times]
            match linear_regression(x_values, y_values, confidence):
                case None:
                    print("No regression")
                    return
                case (b0, (b0_low, b0_high)), (b1, (b1_low, b1_high)), (s2, (s2_low, s2_high)):
                    pass
            result = TestRegression(
                name=challenge_method_registration.strategy_name,
                interface=challenge_method_registration.interface,
                scale=challenge_method_registration.scale,
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
            regression_status += ("%s,%s,%s,%d," + "%f,%f,%f," + "%f,%f,%f," + "%f,%f,%f\n") % (
                challenge_method_registration.strategy_name,
                challenge_method_registration.interface,
                challenge_method_registration.scale,
                result.run_count,
                result.b0, result.b0_low, result.b0_high,
                result.b1 * 1e+6, result.b1_low * 1e+6, result.b1_high * 1e+6,
                result.s2, result.s2_low, result.s2_high)
    with open(FINAL_REPORT_FILE_PATH, 'w') as f:
        f.write(summary_status)
        f.write("\n")
        f.write(regression_status)
        f.write("\n")


def read_run_results_old() -> list[SectionPersistedRunResult]:
    test_runs: list[SectionPersistedRunResult] = []
    for engine in CalcEngine:
        run_log_file_path_for_engine = derive_run_log_file_path(engine)
        if run_log_file_path_for_engine is None or not os.path.exists(run_log_file_path_for_engine):
            continue
        with open(run_log_file_path_for_engine, 'rt') as f:
            for line in f:
                line = line.rstrip()
                if line.startswith("Working"):
                    print("Excluding line: " + line)
                    continue
                if line.find(',') < 0:
                    print("Excluding line: " + line)
                    continue
                fields: list[str] = line.rstrip().split(',')
                status, strategy_name, interface, \
                    num_students, \
                    result_num_source_rows, \
                    section_maximum, \
                    elapsed_time, \
                    num_output_rows, \
                    finished_at, *_rest \
                    = tuple(fields)
                if status != 'success':
                    print("Excluding line: " + line)
                    continue
                result = SectionPersistedRunResult(
                    num_source_rows=int(result_num_source_rows),
                    strategy_name=strategy_name,
                    language=LANGUAGE,
                    engine=engine,
                    interface=parse_interface_python(interface, engine),
                    status=status,
                    section_maximum=int(section_maximum),
                    num_students=int(num_students),
                    elapsed_time=float(elapsed_time),
                    num_output_rows=int(num_output_rows),
                    finished_at=finished_at,
                )
                test_runs.append(result)

    return test_runs


def analyze_run_results_new():
    summary_status: list[SummarizedPerformanceOfMethodAtDataSize] = []
    engine = CalcEngine.PYSPARK
    challenge_method_list = STRATEGIES_USING_PYSPARK_REGISTRY
    reader = SectionPythonPersistedRunResultLog(engine)
    raw_test_runs = reader.read_run_result_file()
    structured_test_results = reader.structure_test_results(
        challenge_method_list=challenge_method_list,
        expected_sizes=EXPECTED_SIZES,
        regressor_from_run_result=regressor_from_run_result,
        test_runs=raw_test_runs,
    )
    summary_status.extend(
        reader.do_regression(
            challenge=CHALLENGE,
            engine=engine,
            challenge_method_list=challenge_method_list,
            test_results_by_strategy_name_by_data_size=structured_test_results,
        )
    )
    reader.print_summary(
        rows=summary_status,
        file_report_file_path=FINAL_REPORT_FILE_PATH,
    )


if __name__ == "__main__":
    print(f"Running {__file__}")
    analyze_run_results_new()
    analyze_run_results_old()
    print("Done!")
