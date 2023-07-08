import collections
import math

import numpy
import scipy.stats
from SixFieldTestData import RunResult

from Utils.LinearRegression import linear_regression
from .CondDirectory import implementation_list

TEMP_RESULT_FILE_PATH = "d:/temp/SparkPerfTesting/temp.csv"

def analyze_run_results():
    cond_runs = {}
    with open('Results/cond_runs.csv', 'r') as f:
        for textline in f:
            if textline.startswith('#'):
                print("Excluding line: "+textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: "+textline)
                continue
            fields = textline.rstrip().split(',')
            if len(fields) < 5:
                fields.append('9')
            # print("Found "+";".join(fields))
            cond_method_name, cond_method_interface, result_dataSize, result_elapsedTime, result_recordCount = tuple(
                fields)
            if result_recordCount != '9':
                print("Excluding line: "+textline)
                continue
            if cond_method_name not in cond_runs:
                cond_runs[cond_method_name] = []
            result = RunResult(
                dataSize=int(result_dataSize),
                elapsedTime=float(result_elapsedTime),
                recordCount=int(result_recordCount))
            cond_runs[cond_method_name].append(result)
    CondResult = collections.namedtuple("CondResult",
                                        ["name", "interface",
                                         "b0", "b0_low", "b0_high",
                                         "b1", "b1_low", "b1_high",
                                         "s2", "s2_low", "s2_high"])
    summary_status = ''
    regression_status = ''
    if True:
        cond_results = []
        confidence = 0.95
        summary_status += "%s,%s,%s,%s,%s,%s,%s,%s\n" % (
            'Method', 'Interface',
            'DataSize', 'NumRuns', 'Elapsed Time', 'stdev', 'rl', 'rh'
        )
        regression_status += '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
            'Method', 'Interface',
            'b0_low', 'b0', 'b0_high',
            'b1_low', 'b1', 'b1_high',
            's2_low', 's2', 's2_high')
        for strategy_name in cond_runs:
            cond_method = [x for x in implementation_list if x.strategy_name == strategy_name][0]
            times = cond_runs[strategy_name]
            size_values = set(x.dataSize for x in times)
            for dataSize in set(x.dataSize for x in times):
                ar = [x.elapsedTime for x in times if x.dataSize == dataSize]
                numRuns = len(ar)
                mean = numpy.mean(ar)
                stdev = numpy.std(ar, ddof=1)
                rl, rh = scipy.stats.norm.interval( # type: ignore
                    confidence, loc=mean, scale=stdev/math.sqrt(len(ar)))
                summary_status += "%s,%s,%d,%d,%f,%f,%f,%f\n" % (
                    strategy_name, cond_method.interface,
                    dataSize, numRuns, mean, stdev, rl, rh
                )
            x_values = [math.log10(x.dataSize) for x in times]
            y_values = [math.log10(x.elapsedTime) for x in times]
            (b0, (b0_low, b0_high)), (b1, (b1_low, b1_high)), (s2, (s2_low, s2_high)) = \
                linear_regression(x_values, y_values, confidence)
            # a = numpy.array(y_values)
            # mean, sem, cumm_conf = numpy.mean(a), scipy.stats.sem(a, ddof=1), scipy.stats.t.ppf((1+confidence)/2., len(a)-1)
            # rangelow, rangehigh = \
            #     scipy.stats.t.interval(confidence, len(times)-1, loc=mean, scale=sem)
            result = CondResult(
                name=cond_method.strategy_name,
                interface=cond_method.interface,
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
            regression_status += '%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f\n' % (
                cond_method.strategy_name, cond_method.interface,
                result.b0_low, result.b0, result.b0_high,
                result.b1_low, result.b1, result.b1_high,
                result.s2_low, result.s2, result.s2_high)
    with open('Results/cond_results.csv', 'wt') as f:
        f.write(summary_status)
        f.write("\n")
        f.write(regression_status)
        f.write("\n")
