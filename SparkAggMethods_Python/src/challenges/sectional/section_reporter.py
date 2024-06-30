#!python
# python -m SectionPerfTest.SectionReporter
import math
import os
from typing import NamedTuple, cast

import numpy
import scipy.stats

from challenges.sectional.section_generate_test_data import DataSetDescription
from challenges.sectional.section_record_runs import (FINAL_REPORT_FILE_PATH,
                                                      derive_run_log_file_path)
from challenges.sectional.section_strategy_directory import (
    solutions_using_dask, solutions_using_pyspark, solutions_using_python_only)
from challenges.sectional.section_test_data_types import (DataSet, DataSetData,
                                                          ExecutionParameters,
                                                          RunResult)
from perf_test_common import CalcEngine
from utils.linear_regression import linear_regression


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


def analyze_run_results():  # noqa: C901
    test_runs = read_run_results()
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
                [x for x in solutions_using_dask if x.strategy_name == strategy_name]
                + [x for x in solutions_using_pyspark if x.strategy_name == strategy_name]
                + [x for x in solutions_using_python_only if x.strategy_name == strategy_name])[0]

            size_values = set(x.data.description.num_rows for x in times)
            for dataSize in size_values:
                runs = [x for x in times if x.data.description.num_rows == dataSize]
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
                    len(ar), dataSize,
                    mean, stdev, rl, rh
                )
            x_values = [float(x.data.description.num_rows) for x in times]
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


def read_run_results() -> list[RunResult]:
    test_runs: list[RunResult] = []
    with open('results/temp.csv', 'w') as out_fh:
        for engine in [CalcEngine.PYSPARK, CalcEngine.DASK]:
            run_log_file_path_for_engine = derive_run_log_file_path(engine)
            if not os.path.exists(run_log_file_path_for_engine):
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
                    test_status, test_method_name, test_method_interface, \
                        result_num_students, \
                        result_dataSize, result_section_maximum, \
                        result_elapsedTime, result_recordCount, \
                        _finished_at \
                        = tuple(fields)
                    if test_status != 'success':
                        print("Excluding line: " + line)
                        continue
                    result = RunResult(
                        strategy_name=test_method_name,
                        engine=engine,
                        success=test_status == "success",
                        data=DataSet(
                            description=DataSetDescription(
                                size_code=str(result_num_students),
                                num_rows=int(result_dataSize),
                                num_students=int(result_num_students),
                            ),
                            data=DataSetData(
                                section_maximum=int(result_section_maximum),
                                test_filepath="N/A",
                                target_num_partitions=-1,
                            ),
                            exec_params=ExecutionParameters(
                                default_parallelism=-1,
                                maximum_processable_segment=-1,
                                test_data_folder_location="N/A",
                            )
                        ),
                        elapsed_time=float(result_elapsedTime),
                        record_count=int(result_recordCount))
                    test_runs.append(result)
                    out_fh.write("%s,%s,%s,%d,%d,%f,%s\n" % (
                        engine.value,
                        test_method_name, test_method_interface,
                        result.data.description.num_rows // result.data.data.section_maximum,
                        result.data.data.section_maximum,
                        result.elapsed_time,
                        result.record_count))

    return test_runs


if __name__ == "__main__":
    analyze_run_results()
    print("Done!")
