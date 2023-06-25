import collections
import datetime
from typing import TextIO


from ConditionalPerfTest.CondDirectory import PythonTestMethod

RunResult = collections.namedtuple(
    "RunResult", ["dataSize", "elapsedTime", "recordCount"])



def write_header(file: TextIO):
    print(' strategy,interface,dataSize,relCard,elapsedTime,recordCount,finishedAt,', file=file)
    file.flush()


def write_run_result(cond_method: PythonTestMethod, result: RunResult, file: TextIO):
    print("%s,%s,%d,%f,%d,%s\n" % (
        cond_method.strategy_name, cond_method.interface,
                    result.dataSize, result.elapsedTime, result.recordCount,
        datetime.datetime.now().isoformat(),
    ), file=file)
    file.flush()
