from typing import Dict, List
import math
import os

import numpy
from scipy.stats import norm as scipy_stats_norm  # type: ignore


from PerfTestCommon import FullCondMethod
from .Strategy.Directory import (
    implementation_list, scala_implementation_list)
from .RunResult import RunResult

SCALA_RESULT_FILE_PATH = '../Results/Scala/vanilla_runs_scala.csv'
PYTHON_RESULT_FILE_PATH = 'Results/vanilla_runs.csv'
REPORT_FILE_PATH='../Results/python/vanilla_results_20230618.csv'

def parse_results() -> Dict[str, List[RunResult]]:
    cond_runs = {}
    if os.path.exists(PYTHON_RESULT_FILE_PATH):
        with open(PYTHON_RESULT_FILE_PATH, 'r') as f:
            for textline in f:
                if textline.startswith('#'):
                    # print("Excluding line: "+textline)
                    continue
                if textline.find(',') < 0:
                    print("Excluding line: "+textline)
                    continue
                fields = textline.rstrip().split(',')
                if len(fields) < 5:
                    fields.append('9')
                cond_method_name, cond_method_interface, result_dataSize, result_elapsedTime, result_recordCount = tuple(
                    fields)
                if result_recordCount != '9':
                    print("Excluding line: "+textline)
                    continue
                typed_method_name = f"{cond_method_name}_python"
                if typed_method_name not in cond_runs:
                    cond_runs[typed_method_name] = []
                result = RunResult(
                    dataSize=int(result_dataSize),
                    elapsedTime=float(result_elapsedTime),
                    recordCount=int(result_recordCount))
                cond_runs[typed_method_name].append(result)
    if os.path.exists(SCALA_RESULT_FILE_PATH):
        with open(SCALA_RESULT_FILE_PATH, 'r') as f:
            for textline in f:
                if textline.startswith('#'):
                    print("Excluding line: "+textline)
                    continue
                if textline.startswith(' '):
                    print("Excluding line: "+textline)
                    continue
                if textline.find(',') < 0:
                    print("Excluding line: "+textline)
                    continue
                fields = textline.rstrip().split(',')
                outcome, rawmethod, interface, expectedSize, returnedSize, elapsedTime = tuple(
                    fields)
                if outcome != 'success':
                    print("Excluding line: "+textline)
                    continue
                if returnedSize != '9':
                    print("Excluding line: "+textline)
                    continue
                typed_method_name = f"{rawmethod}_scala"
                if typed_method_name not in cond_runs:
                    cond_runs[typed_method_name] = []
                result = RunResult(
                    dataSize=int(expectedSize),
                    elapsedTime=float(elapsedTime),
                    recordCount=-1)
                cond_runs[typed_method_name].append(result)
    return cond_runs

def do_regression(python_implementation_list, scala_implementation_list, cond_runs):
    summary_status = ''
    regression_status = ''
    # FullCondMethod = collections.namedtuple("FullCondMethod",
    #                                             ["data_name", "raw_method_name", "language", "interface"])
    confidence = 0.95
    sorted_implementation_list = sorted(
        [FullCondMethod(
            data_name=f"{x.name}_{x.language}", raw_method_name=x.name,
            language=x.language, interface=x.interface)
            for x in python_implementation_list + scala_implementation_list],
        key=lambda x: (x.language, x.interface, x.raw_method_name))
    summary_status += ",".join([
        'RunName', 'RawMethod', 'Method', 'Language', 'Interface',
        'DataSize', 'NumRuns', 'Elapsed Time', 'stdev', 'rl', 'rh'])+"\n"
    for method in sorted_implementation_list:
        times = cond_runs[method.data_name] if method.data_name in cond_runs else []
        size_values = set(x.dataSize for x in times)
        for dataSize in sorted(size_values):
            ar = [x.elapsedTime for x in times if x.dataSize == dataSize]
            numRuns = len(ar)
            mean = numpy.mean(ar)
            stdev = numpy.std(ar, ddof=1)
            rl, rh = scipy_stats_norm.interval(
                confidence, loc=mean, scale=stdev/math.sqrt(len(ar)))
            summary_status += "%s,%s,%s,%s,%s,%d,%d,%f,%f,%f,%f\n" % (
                method.data_name,
                method.raw_method_name,
                method.raw_method_name.replace("vanilla_", ""),
                method.language,
                method.interface,
                dataSize, numRuns, mean, stdev, rl, rh
            )
    return summary_status

def DoAnalysis():
    cond_runs = parse_results()
    summary_status = do_regression(implementation_list, scala_implementation_list, cond_runs)
        
    with open(REPORT_FILE_PATH, 'wt') as f:
        f.write(summary_status)
        f.write("\n")

if __name__ == "__main__":
    DoAnalysis()
