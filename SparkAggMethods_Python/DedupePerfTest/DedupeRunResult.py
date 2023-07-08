from dataclasses import dataclass
import datetime
from typing import Iterable, TextIO

from .DedupeDataTypes import PythonTestMethod

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


@dataclass(frozen=True)
class PersistedRunResult:
    status: str
    stategy_name: str
    interface: str
    numSources: int
    dataSize: int
    dataSizeExp: int
    actualNumPeople: int
    elapsedTime: float
    foundNumPeople: int
    IsCloudMode: bool
    CanAssumeNoDupesPerPartition: bool


def write_header(file: TextIO):
    print(",".join((
        " result",
        "strategy", "interface", "numSources",
        "dataSize", "dataSizeExp", "actualNumPeople",
        "elapsedTime", "foundNumPeople",
        "IsCloudMode", "CanAssumeNoDupesPerPartition",
        "finishedAt")), file=file)
    file.flush()


def write_run_result(
        success: bool,
        test_method: PythonTestMethod,
        result: RunResult,
        file: TextIO
):
    print(",".join([str(x) for x in [
        "success" if success else "failure",
        test_method.strategy_name, test_method.interface, result.numSources,
        result.dataSize, result.dataSizeExp, result.actualNumPeople,
        result.elapsedTime, result.foundNumPeople,
        "TRUE" if result.IsCloudMode else "FALSE",
        "TRUE" if result.CanAssumeNoDupesPerPartition else "FALSE",
        datetime.datetime.now().isoformat()]]
    ), file=file)
    file.flush()


def read_result_file() -> Iterable[PersistedRunResult]:
    with open(RESULT_FILE_PATH, 'r') as f:
        for textline in f:
            textline = textline.rstrip()
            if textline.startswith("Working"):
                print("Excluding line: " + textline)
                continue
            if textline.startswith("#"):
                print("Excluding line: " + textline)
                continue
            if textline.find(',') < 0:
                print("Excluding line: " + textline)
                continue
            fields = textline.split(',')
            test_status, test_method_name, test_method_interface, \
                result_numSources, \
                result_dataSize, result_dataSizeExp, \
                result_actualNumPeople, \
                result_elapsedTime, result_foundNumPeople, \
                result_IsCloudMode, result_CanAssumeNoDupesPerPartition, \
                = tuple(fields)
            if test_status != 'success':
                print("Excluding line: " + textline)
                continue
            result = PersistedRunResult(
                status=test_status,
                stategy_name=test_method_name,
                interface=test_method_interface,
                numSources=int(result_numSources),
                dataSize=int(result_dataSize),
                dataSizeExp=int(result_dataSizeExp),
                actualNumPeople=int(result_actualNumPeople),
                elapsedTime=float(result_elapsedTime),
                foundNumPeople=int(result_foundNumPeople),
                IsCloudMode=bool(result_IsCloudMode),
                CanAssumeNoDupesPerPartition=bool(
                    result_CanAssumeNoDupesPerPartition),
            )
            yield result
