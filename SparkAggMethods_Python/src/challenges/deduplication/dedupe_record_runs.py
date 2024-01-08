import datetime
import os
from dataclasses import dataclass
from typing import Iterable, TextIO

from challenges.deduplication.dedupe_test_data_types import PysparkTestMethod
from perf_test_common import CalcEngine
from utils.utils import always_true, root_folder_abs_path

T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/dedupe_pyspark_runs.csv'
T_PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/dedupe_dask_runs.csv'
FINAL_REPORT_FILE_PATH = 'results/dedupe_results.csv'


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


def derive_run_log_file_path(
        engine: CalcEngine,
) -> str:
    match engine:
        case  CalcEngine.PYSPARK:
            run_log = T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH
        case CalcEngine.DASK:
            run_log = T_PYTHON_DASK_RUN_LOG_FILE_PATH
        case _:
            raise ValueError(f"Unknown engine: {engine}")
    return os.path.join(
        root_folder_abs_path(),
        run_log)


def regressor_from_run_result(
        result: PersistedRunResult,
) -> int:
    return result.dataSize


def write_header(
        file: TextIO,
) -> None:
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
        test_method: PysparkTestMethod,
        result: RunResult,
        file: TextIO
) -> None:
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


def read_result_file(
        engine: CalcEngine,
) -> Iterable[PersistedRunResult]:
    with open(derive_run_log_file_path(engine), 'r') as f:
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
                _finishedAt, *_rest \
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
