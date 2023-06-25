
import collections
import math

import numpy
import scipy

from LinearRegression import linear_regression

from .BiLevelDirectory import implementation_list
from .BiLevelRunResult import (FINAL_REPORT_FILE_PATH, RESULT_FILE_PATH,
                               RunResult)

TEMP_RESULT_FILE_PATH = "d:/temp/SparkPerfTesting/temp.csv"

def analyze_run_results():
    cond_runs = {}
    with open(RESULT_FILE_PATH, 'r', encoding='utf-8-sig') as f, \
            open(TEMP_RESULT_FILE_PATH, 'w') as fout:
        for textline in f:
            if textline.startswith('#'):
                print("Excluding line: "+textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: "+textline)
                continue
            fields = textline.rstrip().split(',')
            if len(fields) < 6:
                fields.append('3')
            cond_method_name, cond_method_interface, result_dataSize, result_relCard, result_elapsedTime, result_recordCount = fields
            if result_recordCount != '3':
                print("Excluding line: "+textline)
                continue
            result = RunResult(
                dataSize=int(result_dataSize),
                relCard=int(result_relCard),
                elapsedTime=float(result_elapsedTime),
                recordCount=int(result_recordCount))
            if cond_method_name not in cond_runs:
                cond_runs[cond_method_name] = []
            cond_runs[cond_method_name].append(result)
            fout.write("%s,%s,%d,%d,%f,%d\n" % (cond_method_name, cond_method_interface,
                       result.dataSize, result.relCard, result.elapsedTime, result.recordCount))
    CondResult = collections.namedtuple("CondResult",
                                        ["name", "interface", "run_count",
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
            'NumRuns', 'relCard', 'Elapsed Time', 'stdev', 'rl', 'rh'
        )
        regression_status += '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' % (
            'Method', 'Interface',
            'b0_low', 'b0', 'b0_high',
            'b1_low', 'b1', 'b1_high',
            's2_low', 's2', 's2_high')
        # f.write(("%s,%s,%s,"+"%s,%s,%s,"+"%s,%s,%s,"+"%s,%s,%s\n")%(
        #     'RawMethod', 'interface', 'run_count',
        #     'b0', 'b0 lo', 'b0 hi',
        #     'b1M', 'b1M lo', 'b1M hi',
        #     's2', 's2 lo', 's2 hi'))
        # f.write(("%s,%s,%s,"+"%s,%s,%s,"+"%s,%s\n")% (
        #     'RawMethod', 'interface', 'run_count',
        #     'relCard', 'mean', 'stdev',
        #     'rl', 'rh'
        # ))
        for name in cond_runs:
            print("Looking to analyze %s" % name)
            cond_method = [x for x in implementation_list if x.strategy_name == name][0]
            times = cond_runs[name]
            size_values = set(x.relCard for x in times)
            for relCard in size_values:
                ar = [x.elapsedTime for x in times if x.relCard == relCard]
                numRuns = len(ar)
                mean = numpy.mean(ar)
                stdev = numpy.std(ar, ddof=1)
                rl, rh = scipy.stats.norm.interval( # type: ignore
                    confidence, loc=mean, scale=stdev/math.sqrt(len(ar)))
                # f.write(("%s,%s,"+"%d,%d,"+"%f,%f,%f,%f\n")%(
                #     name, cond_method.interface,
                #     numRuns, relCard,
                #     mean, stdev, rl, rh
                # ))
                summary_status += "%s,%s,%d,%d,%f,%f,%f,%f\n" % (
                    name, cond_method.interface,
                    numRuns, relCard, mean, stdev, rl, rh
                )
            x_values = [float(x.relCard) for x in times]
            y_values = [float(x.elapsedTime) for x in times]
            (b0, (b0_low, b0_high)), (b1, (b1_low, b1_high)), (s2, (s2_low, s2_high)) = \
                linear_regression(x_values, y_values, confidence)
            result = CondResult(
                name=cond_method.strategy_name,
                interface=cond_method.interface,
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
            cond_results.append(result)
            # f.write(("%s,%s,%d,"+"%f,%f,%f,"+"%f,%f,%f,"+"%f,%f,%f\n")%(
            #     cond_method.name, cond_method.interface, result.run_count,
            #     result.b0, result.b0_low, result.b0_high,
            #     result.b1*1e+6, result.b1_low*1e+6, result.b1_high*1e+6,
            #     result.s2, result.s2_low, result.s2_high))
            regression_status += '%s,%s,%f,%f,%f,%f,%f,%f,%f,%f,%f\n' % (
                cond_method.strategy_name, cond_method.interface,
                result.b0_low, result.b0, result.b0_high,
                result.b1_low, result.b1, result.b1_high,
                result.s2_low, result.s2, result.s2_high)
    with open(FINAL_REPORT_FILE_PATH, 'w') as f:
        f.write(summary_status)
        f.write("\n")
        f.write(regression_status)
        f.write("\n")


if __name__ == "__main__":
    # DoPostProcess_Unknown_skipped()
    analyze_run_results()
