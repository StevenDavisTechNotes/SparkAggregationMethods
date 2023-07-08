#!python
# python -m DedupePerfTest.DedupeReporter
import collections
import math

import scipy.stats
import numpy

from Utils.LinearRegression import linear_regression
from .DedupeDirectory import implementation_list
from .DedupeRunResult import FINAL_REPORT_FILE_PATH, read_result_file

TEMP_RESULT_FILE_PATH = "d:/temp/SparkPerfTesting/temp.csv"


def analyze_run_results():
    test_runs = {}
    with open(TEMP_RESULT_FILE_PATH, 'w') as fout:
        for result in read_result_file():
            if result.stategy_name not in test_runs:
                test_runs[result.stategy_name] = []
            test_runs[result.stategy_name].append(result)
            fout.write("%s,%s,%d,%d,%d,%d,%f\n" % (
                result.stategy_name, result.interface,
                result.numSources, result.actualNumPeople,
                result.dataSize, result.dataSizeExp,
                result.elapsedTime))
    if len(test_runs) < 1:
        print("no tests")
        return
    if any([len(x) for x in test_runs.values()]) < 10:
        print("not enough data ", [len(x) for x in test_runs.values()])
        return
    TestRegression = collections.namedtuple("TestRegression",
                                            ["name", "interface", "run_count",
                                             "b0", "b0_low", "b0_high",
                                             "b1", "b1_low", "b1_high",
                                             "s2", "s2_low", "s2_high"])
    summary_status = ''
    regression_status = ''
    if True:
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
                summary_status += ("%s,%s," + "%d,%d," + "%f,%f,%f,%f\n") % (
                    name, test_method.interface,
                    len(ar), dataSize,
                    mean, stdev, rl, rh
                )
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
    analyze_run_results()
