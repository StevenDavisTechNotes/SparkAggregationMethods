#!python
# python -m SectionPerfTest.SectionReporter
import collections
import math
import os
from typing import List, cast

import numpy
import scipy.stats

from PerfTestCommon import CalcEngine
from SectionPerfTest.SectionDirectory import (dask_implementation_list,
                                              pyspark_implementation_list)
from SectionPerfTest.SectionRunResult import (FINAL_REPORT_FILE_PATH,
                                              derive_run_log_file_path)
from SectionPerfTest.SectionTestData import DataSetDescription
from SectionPerfTest.SectionTypeDefs import (DataSet, DataSetData,
                                             ExecutionParameters, RunResult)
from Utils.LinearRegression import linear_regression

TestRegression = collections.namedtuple(
    "TestRegression",
    ["name", "interface", "scale", "run_count",
     "b0", "b0_low", "b0_high",
     "b1", "b1_low", "b1_high",
     "s2", "s2_low", "s2_high"])


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
        results_for_staregy = [x for x in test_runs if x.strategy_name == strategy_name]
        test_engines = {x.engine for x in results_for_staregy}
        for engine in test_engines:
            times = [x for x in results_for_staregy if x.engine == engine]
            if len(times) == 0:
                continue
            if len(times) < 10:
                print(f"not enough data {test_strategy_names}/{engine.value}")
                return
    for strategy_name in test_strategy_names:
        results_for_staregy = [x for x in test_runs if x.strategy_name == strategy_name]
        test_engines = {x.engine for x in results_for_staregy}
        for engine in test_engines:
            times = [x for x in results_for_staregy if x.engine == engine]
            if len(times) == 0:
                continue
            test_method = ([x for x in pyspark_implementation_list if x.strategy_name == strategy_name]
                           + [x for x in dask_implementation_list if x.strategy_name == strategy_name])[0]

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
                    test_method.strategy_name, test_method.interface, test_method.scale,
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
                name=test_method.strategy_name,
                interface=test_method.interface,
                scale=test_method.scale,
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
                test_method.strategy_name, test_method.interface, test_method.scale, result.run_count,
                result.b0, result.b0_low, result.b0_high,
                result.b1 * 1e+6, result.b1_low * 1e+6, result.b1_high * 1e+6,
                result.s2, result.s2_low, result.s2_high)
    with open(FINAL_REPORT_FILE_PATH, 'w') as f:
        f.write(summary_status)
        f.write("\n")
        f.write(regression_status)
        f.write("\n")


def read_run_results() -> List[RunResult]:
    test_runs: List[RunResult] = []
    with open('Results/temp.csv', 'w') as fout:
        for engine in [CalcEngine.PYSPARK, CalcEngine.DASK]:
            run_log_file_path_for_engine = derive_run_log_file_path(engine)
            if not os.path.exists(run_log_file_path_for_engine):
                continue
            with open(run_log_file_path_for_engine, 'rt') as f:
                for textline in f:
                    textline = textline.rstrip()
                    if textline.startswith("Working"):
                        print("Excluding line: " + textline)
                        continue
                    if textline.find(',') < 0:
                        print("Excluding line: " + textline)
                        continue
                    fields: List[str] = textline.rstrip().split(',')
                    test_status, test_method_name, test_method_interface, \
                        result_num_students, \
                        result_dataSize, result_section_maximum, \
                        result_elapsedTime, result_recordCount, \
                        _finished_at \
                        = tuple(fields)
                    if test_status != 'success':
                        print("Excluding line: " + textline)
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
                                DefaultParallelism=-1,
                                MaximumProcessableSegment=-1,
                                TestDataFolderLocation="N/A",
                            )
                        ),
                        elapsed_time=float(result_elapsedTime),
                        record_count=int(result_recordCount))
                    test_runs.append(result)
                    fout.write("%s,%s,%s,%d,%d,%f,%s\n" % (
                        engine.value,
                        test_method_name, test_method_interface,
                        result.data.description.num_rows // result.data.data.section_maximum,
                        result.data.data.section_maximum,
                        result.elapsed_time,
                        result.record_count))

    return test_runs


if __name__ == "__main__":
    analyze_run_results()
