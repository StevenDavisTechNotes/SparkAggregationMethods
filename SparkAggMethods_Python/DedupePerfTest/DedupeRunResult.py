from dataclasses import dataclass
import datetime
from typing import Iterable, TextIO

from Utils.Utils import always_true

from .DedupeDataTypes import DataSet, PythonTestMethod

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


@dataclass(frozen=True)
class PersistedRunResult:
    status: str
    strategy_name: str
    interface: str
    numSources: int
    dataSize: int
    dataSizeExp: int
    actualNumPeople: int
    elapsedTime: float
    foundNumPeople: int
    IsCloudMode: bool
    CanAssumeNoDupesPerPartition: bool


EXPECTED_NUM_RECORDS = sorted([
    num_data_points
    for data_size_exp in range(0, 5)
    if always_true(num_people := 10**data_size_exp)
    for num_sources in [2, 3, 6]
    if always_true(num_data_points := (
        (0 if num_sources < 1 else num_people)
        + (0 if num_sources < 2 else max(1, 2 * num_people // 100))
        + (0 if num_sources < 3 else num_people)
        + 3 * (0 if num_sources < 6 else num_people)
    ))
    if num_data_points < 50200
])


def regressor_from_run_result(result: PersistedRunResult) -> int:
    return result.dataSize


def infeasible(strategy_name: str, data_set: DataSet) -> bool:
    match strategy_name:
        case 'dedupe_pandas':
            return data_set.data_size > 50200
        case 'dedupe_fluent_nested_python':
            return data_set.data_size > 502000  # 20200
        case 'dedupe_fluent_nested_withCol':
            return data_set.data_size > 20200
        case 'dedupe_fluent_windows':
            return data_set.data_size > 50200  # 20200
        case 'dedupe_rdd_groupby':
            return data_set.data_size > 50200  # takes too long otherwise
        case 'dedupe_rdd_mappart':
            return data_set.data_size > 502000  # takes too long otherwise
        case 'dedupe_rdd_reduce':
            return data_set.data_size > 502000  # takes too long otherwise
        case _:
            raise ValueError(f"Unknown strategy: {strategy_name}")


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
            test_status, strategy_name, interface, \
                result_numSources, \
                result_dataSize, result_dataSizeExp, \
                result_actualNumPeople, \
                result_elapsedTime, result_foundNumPeople, \
                result_IsCloudMode, result_CanAssumeNoDupesPerPartition, \
                finishedAt, *rest \
                = tuple(fields)
            if test_status != 'success':
                print("Excluding line: " + textline)
                continue
            result = PersistedRunResult(
                status=test_status,
                strategy_name=strategy_name,
                interface=interface,
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
