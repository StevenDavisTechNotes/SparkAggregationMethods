import collections
import math

import numpy

from .DedupeDirectory import implementation_list
from .DedupeRunResult import RunResult


def DoAnalysis():
    import scipy.stats

    from LinearRegression import linear_regression
    test_runs = {}
    with open('Results/dedupe_runs_tail.csv', 'r') as f, \
            open('Results/temp.csv', 'w') as fout:
        for textline in f:
            if textline.startswith("Working"):
                print("Excluding line: "+textline)
                continue
            if textline.startswith("#"):
                print("Excluding line: "+textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: "+textline)
                continue
            fields = textline.rstrip().split(',')
            test_status, test_method_name, test_method_interface, \
                result_numSources, \
                result_dataSize, result_dataSizeExp, \
                result_actualNumPeople, \
                result_elapsedTime, result_foundNumPeople, \
                result_IsCloudMode, result_CanAssumeNoDupesPerPartition, \
                _result_executed_at \
                = tuple(fields)
            if test_status != 'success':
                print("Excluding line: "+textline)
                continue
            result_numSources = int(result_numSources)
            result_dataSize = int(result_dataSize)
            result_dataSizeExp = int(result_dataSizeExp)
            result_actualNumPeople = int(result_actualNumPeople)
            result_elapsedTime = float(result_elapsedTime)
            result_foundNumPeople = int(result_foundNumPeople)
            result_IsCloudMode = bool(result_IsCloudMode)
            result_CanAssumeNoDupesPerPartition = bool(
                result_CanAssumeNoDupesPerPartition)
            result = RunResult(
                numSources=result_numSources,
                actualNumPeople=result_actualNumPeople,
                dataSize=result_dataSize,
                dataSizeExp=result_dataSizeExp,
                elapsedTime=result_elapsedTime,
                foundNumPeople=result_foundNumPeople,
                IsCloudMode=result_IsCloudMode,
                CanAssumeNoDupesPerPartition=result_CanAssumeNoDupesPerPartition)
            if test_method_name not in test_runs:
                test_runs[test_method_name] = []
            test_runs[test_method_name].append(result)
            fout.write("%s,%s,%d,%d,%d,%d,%f\n" % (
                test_method_name, test_method_interface,
                result.numSources, result.actualNumPeople,
                result.dataSize, result.dataSizeExp,
                result.elapsedTime))
    # print(test_runs)
    if len(test_runs) < 1:
        print("no tests")
        return
    if min([len(x) for x in test_runs.values()]) < 10:
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
        regression_status += ("%s,%s,%s,"+"%s,%s,%s,"+"%s,%s,%s,"+"%s,%s,%s\n") % (
            'RawMethod', 'interface', 'run_count',
            'b0', 'b0 lo', 'b0 hi',
            'b1M', 'b1M lo', 'b1M hi',
            's2', 's2 lo', 's2 hi')
        summary_status += ("%s,%s,%s,"+"%s,%s,%s,"+"%s,%s\n") % (
            'RawMethod', 'interface', 'run_count',
            'DataSize', 'mean', 'stdev',
            'rl', 'rh')
        for name in test_runs:
            print("Looking to analyze %s" % name)
            test_method = [x for x in implementation_list if x.name == name][0]
            times = test_runs[name]
            size_values = set(x.dataSize for x in times)
            for dataSize in size_values:
                ar = [x.elapsedTime for x in times if x.dataSize == dataSize]
                mean = numpy.mean(ar)
                stdev = numpy.std(ar, ddof=1)
                rl, rh = scipy.stats.norm.interval( # type: ignore
                    confidence, loc=mean, scale=stdev/math.sqrt(len(ar)))
                summary_status += ("%s,%s,"+"%d,%d,"+"%f,%f,%f,%f\n") % (
                    name, test_method.interface,
                    len(ar), dataSize,
                    mean, stdev, rl, rh
                )
            x_values = [math.log10(x.dataSize) for x in times]
            y_values = [math.log10(x.elapsedTime) for x in times]
            (b0, (b0_low, b0_high)), (b1, (b1_low, b1_high)), (s2, (s2_low, s2_high)) = \
                linear_regression(x_values, y_values, confidence)
            result = TestRegression(
                name=test_method.name,
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
            regression_status += ("%s,%s,%d,"+"%f,%f,%f,"+"%f,%f,%f,"+"%f,%f,%f\n") % (
                test_method.name, test_method.interface, result.run_count,
                result.b0, result.b0_low, result.b0_high,
                result.b1*1e+6, result.b1_low*1e+6, result.b1_high*1e+6,
                result.s2, result.s2_low, result.s2_high)
    with open('Results/dedupe_results.csv', 'w') as f:
        f.write(summary_status)
        f.write("\n")
        f.write(regression_status)
        f.write("\n")
#
