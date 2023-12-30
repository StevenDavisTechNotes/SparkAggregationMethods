import math
import os
from typing import NamedTuple, cast

import numpy
import scipy.stats

from challenges.conditional.conditional_record_runs import (
    FINAL_REPORT_FILE_PATH, derive_run_log_file_path)
from challenges.conditional.conditional_strategy_directory import \
    pyspark_implementation_list
from perf_test_common import CalcEngine
from six_field_test_data.six_test_data_types import RunResult
from utils.linear_regression import linear_regression

TEMP_RESULT_FILE_PATH = "d:/temp/SparkPerfTesting/temp.csv"


class CondResult(NamedTuple):
    name: str
    interface: str
    b0: float
    b0_low: float
    b0_high: float
    b1: float
    b1_low: float
    b1_high: float
    s2: float
    s2_low: float
    s2_high: float


def analyze_run_results():
    cond_runs = read_run_files()
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
            cond_method = [
                x for x in pyspark_implementation_list if x.strategy_name == strategy_name][0]
            times = cond_runs[strategy_name]
            for dataSize in set(x.dataSize for x in times):
                runs = [x for x in times if x.dataSize == dataSize]
                ar: numpy.ndarray[float, numpy.dtype[numpy.float64]] \
                    = numpy.asarray([x.elapsedTime for x in runs], dtype=float)
                numRuns = len(runs)
                mean = numpy.mean(ar)
                stdev = cast(float, numpy.std(ar, ddof=1))
                rl, rh = scipy.stats.norm.interval(  # type: ignore
                    confidence, loc=mean, scale=stdev / math.sqrt(numRuns))
                summary_status += "%s,%s,%d,%d,%f,%f,%f,%f\n" % (
                    strategy_name, cond_method.interface,
                    dataSize, numRuns, mean, stdev, rl, rh
                )
            x_values = [math.log10(x.dataSize) for x in times]
            y_values = [math.log10(x.elapsedTime) for x in times]
            match linear_regression(x_values, y_values, confidence):
                case None:
                    print("No regression")
                    return
                case (b0, (b0_low, b0_high)), (b1, (b1_low, b1_high)), (s2, (s2_low, s2_high)):
                    pass
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
    with open(FINAL_REPORT_FILE_PATH, 'w') as f:
        f.write(summary_status)
        f.write("\n")
        f.write(regression_status)
        f.write("\n")


def read_run_files():
    cond_runs = {}
    for engine in [CalcEngine.PYSPARK, CalcEngine.DASK]:
        run_log_file_path = derive_run_log_file_path(engine)
        if not os.path.exists(run_log_file_path):
            continue
        with open(run_log_file_path, 'r') as f:
            for textline in f:
                if textline.startswith('#'):
                    print("Excluding line: " + textline)
                    continue
                if textline.find(',') < 0:
                    print("Excluding line: " + textline)
                    continue
                fields = textline.rstrip().split(',')
                if len(fields) < 5:
                    fields.append('9')
                strategy_name, _interface, result_dataSize, result_elapsedTime, result_recordCount = tuple(
                    fields)
                if result_recordCount != '9':
                    print("Excluding line: " + textline)
                    continue
                if strategy_name not in cond_runs:
                    cond_runs[strategy_name] = []
                result = RunResult(
                    engine=engine,
                    dataSize=int(result_dataSize),
                    elapsedTime=float(result_elapsedTime),
                    recordCount=int(result_recordCount))
                cond_runs[strategy_name].append(result)
    return cond_runs
