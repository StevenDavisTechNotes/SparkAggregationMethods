#!python
# python -m SectionPerfTest.SectionReporter
import collections
import math
from typing import List

import numpy
import scipy.stats

from Utils.LinearRegression import linear_regression

from SectionPerfTest.SectionDirectory import implementation_list
from SectionPerfTest.SectionRunResult import FINAL_REPORT_FILE_PATH, PYTHON_RESULT_FILE_PATH
from SectionPerfTest.SectionTestData import DataSetDescription
from SectionPerfTest.SectionTypeDefs import DataSet, DataSetData, ExecutionParameters, RunResult

TestRegression = collections.namedtuple(
    "TestRegression",
    ["name", "interface", "scale", "run_count",
     "b0", "b0_low", "b0_high",
     "b1", "b1_low", "b1_high",
     "s2", "s2_low", "s2_high"])


def analyze_run_results():  # noqa: C901
    test_runs = {}
    with open(PYTHON_RESULT_FILE_PATH, 'rt') as f, \
            open('Results/temp.csv', 'w') as fout:
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
            if test_method_name not in test_runs:
                test_runs[test_method_name] = []
            test_runs[test_method_name].append(result)
            fout.write("%s,%s,%d,%d,%f,%s\n" % (
                test_method_name, test_method_interface,
                result.data.description.num_rows // result.data.data.section_maximum,
                result.data.data.section_maximum,
                result.elapsed_time,
                result.record_count))
    if len(test_runs) < 1:
        print("no tests")
        return
    if min([len(x) for x in test_runs.values()]) < 10:
        print("not enough data ", [len(x) for x in test_runs.values()])
        return

    summary_status = ''
    regression_status = ''
    if True:
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
        for name in test_runs:
            print("Looking to analyze %s" % name)
            test_method = [
                x for x in implementation_list if x.strategy_name == name][0]
            times = test_runs[name]
            size_values = set(x.dataSize for x in times)
            for dataSize in size_values:
                ar = [x.elapsedTime for x in times if x.dataSize == dataSize]
                mean = numpy.mean(ar)
                stdev = numpy.std(ar, ddof=1)
                rl, rh = scipy.stats.norm.interval(  # type: ignore
                    confidence, loc=mean, scale=stdev / math.sqrt(len(ar)))
                summary_status += ("%s,%s,%s," + "%d,%d," + "%f,%f,%f,%f\n") % (
                    name, test_method.interface, test_method.scale,
                    len(ar), dataSize,
                    mean, stdev, rl, rh
                )
            x_values = [float(x.dataSize) for x in times]
            y_values = [x.elapsedTime for x in times]
            (b0, (b0_low, b0_high)), (b1, (b1_low, b1_high)), (s2, (s2_low, s2_high)) = \
                linear_regression(x_values, y_values, confidence)
            # a = numpy.array(y_values)
            # mean, sem, cumm_conf = numpy.mean(a),
            # scipy.stats.sem(a, ddof=1), scipy.stats.t.ppf((1+confidence)/2., len(a)-1)
            # rangelow, rangehigh = \
            #     scipy.stats.t.interval(confidence, len(times)-1, loc=mean, scale=sem)
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


if __name__ == "__main__":
    analyze_run_results()
