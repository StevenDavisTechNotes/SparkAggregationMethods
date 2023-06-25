from typing import TextIO

from dataclasses import dataclass

from pyspark.sql import DataFrame as spark_DataFrame

from DedupePerfTest.DedupeDirectory import PythonTestMethod


RESULT_FILE_PATH = 'Results/dedupe_runs.csv'
FINAL_REPORT_FILE_PATH = '../Results/python/dedupe_results_20230618.csv'


@dataclass(frozen=True)
class RunResult:
    numSources: int
    actualNumPeople: int
    dataSize: int
    dataSizeExp: int
    elapsedTime: float
    foundNumPeople: int
    IsCloudMode: bool
    CanAssumeNoDupesPerPartition: bool

# RunResult = collections.namedtuple("RunResult", ["numSources", "actualNumPeople", "dataSize",
#                                    "dataSizeExp", "elapsedTime", "foundNumPeople", "IsCloudMode", "CanAssumeNoDupesPerPartition"])


def write_header(file: TextIO):
    print(",".join((
        " result",
        "strategy", "interface", "numSources",
        "dataSize", "dataSizeExp", "actualNumPeople",
        "elapsedTime", "foundNumPeople",
        "IsCloudMode", "CanAssumeNoDupesPerPartition")), file=file)
    file.flush()


def write_failed_run(test_method: PythonTestMethod,
                     result: RunResult, file: TextIO):
    print("failure,%s,%s,%d,%d,%d,%d,%f,%d" % (
        test_method.strategy_name, test_method.interface,
        result.numSources, result.dataSize, result.dataSizeExp, result.actualNumPeople,
        result.elapsedTime, result.foundNumPeople), file=file)
    file.flush()


def write_run_result(
        success: bool, test_method: PythonTestMethod, result: RunResult, file: TextIO):
    print("%s,%s,%s,%d,%d,%d,%d,%f,%d,%s,%s" % (
        "success" if success else "failure",
        test_method.strategy_name, test_method.interface, result.numSources,
        result.dataSize, result.dataSizeExp, result.actualNumPeople,
        result.elapsedTime, result.foundNumPeople,
        "TRUE" if result.IsCloudMode else "FALSE",
        "TRUE" if result.CanAssumeNoDupesPerPartition else "FALSE"),
        file=file)
    file.flush()
