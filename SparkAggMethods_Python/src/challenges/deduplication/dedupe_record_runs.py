import datetime
import os
from dataclasses import dataclass
from typing import Iterable, TextIO

from src.challenges.deduplication.dedupe_test_data_types import ChallengeMethodPythonPysparkRegistration
from src.perf_test_common import CalcEngine
from src.utils.utils import always_true, root_folder_abs_path

T_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/dedupe_pyspark_runs.csv'
T_PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/dedupe_dask_runs.csv'
FINAL_REPORT_FILE_PATH = 'results/dedupe_results.csv'


@dataclass(frozen=True)
class DedupeRunResult:
    num_sources: int
    num_people_actual: int
    num_data_points: int
    data_size_exponent: int
    elapsed_time: float
    num_people_found: int
    in_cloud_mode: bool
    can_assume_no_duplicates_per_partition: bool


@dataclass(frozen=True)
class DedupePersistedRunResult:
    status: str
    strategy_name: str
    interface: str
    num_sources: int
    num_data_points: int
    data_size_exponent: int
    num_people_actual: int
    elapsed_time: float
    num_people_found: int
    in_cloud_mode: bool
    can_assume_no_duplicates_per_partition: bool


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


def derive_run_log_file_path_for_recording(
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


def derive_run_log_file_path_for_reading(
        engine: CalcEngine,
) -> str | None:
    match engine:
        case CalcEngine.DASK | CalcEngine.PYSPARK | CalcEngine.PYTHON_ONLY:
            return derive_run_log_file_path_for_recording(engine)
        case CalcEngine.SCALA_SPARK:
            return None
        case _:
            raise ValueError(f"Unknown engine: {engine}")


def regressor_from_run_result(
        result: DedupePersistedRunResult,
) -> int:
    return result.num_data_points


def write_header(
        file: TextIO,
) -> None:
    print(",".join((
        " result",
        "strategy", "interface", "num_sources",
        "num_data_points", "data_size_exponent", "num_people_actual",
        "elapsed_time", "num_people_found",
        "in_cloud_mode", "can_assume_no_duplicates_per_partition",
        "finished_at")), file=file)
    file.flush()


def write_run_result(
        success: bool,
        challenge_method_registration: ChallengeMethodPythonPysparkRegistration,
        result: DedupeRunResult,
        file: TextIO
) -> None:
    print(",".join([str(x) for x in [
        "success" if success else "failure",
        challenge_method_registration.strategy_name, challenge_method_registration.interface, result.num_sources,
        result.num_data_points, result.data_size_exponent, result.num_people_actual,
        result.elapsed_time, result.num_people_found,
        "TRUE" if result.in_cloud_mode else "FALSE",
        "TRUE" if result.can_assume_no_duplicates_per_partition else "FALSE",
        datetime.datetime.now().isoformat()]]
    ), file=file)
    file.flush()


def read_run_result_file(
        engine: CalcEngine,
) -> Iterable[DedupePersistedRunResult]:
    log_file_path = derive_run_log_file_path_for_reading(engine)
    if log_file_path is None or not os.path.exists(log_file_path):
        print(f"File not found: {log_file_path}")
        return
    with open(log_file_path, 'r') as f:
        for line in f:
            line = line.rstrip()
            if line.startswith("Working"):
                print("Excluding line: " + line)
                continue
            if line.startswith("#"):
                print("Excluding line: " + line)
                continue
            if line.find(',') < 0:
                print("Excluding line: " + line)
                continue
            fields = line.split(',')
            test_status, strategy_name, interface, \
                result_numSources, \
                result_dataSize, result_dataSizeExp, \
                result_actualNumPeople, \
                result_elapsedTime, result_foundNumPeople, \
                result_IsCloudMode, result_CanAssumeNoDupesPerPartition, \
                _finishedAt, *_rest \
                = tuple(fields)
            if test_status != 'success':
                print("Excluding line: " + line)
                continue
            result = DedupePersistedRunResult(
                status=test_status,
                strategy_name=strategy_name,
                interface=interface,
                num_sources=int(result_numSources),
                num_data_points=int(result_dataSize),
                data_size_exponent=int(result_dataSizeExp),
                num_people_actual=int(result_actualNumPeople),
                elapsed_time=float(result_elapsedTime),
                num_people_found=int(result_foundNumPeople),
                in_cloud_mode=bool(result_IsCloudMode),
                can_assume_no_duplicates_per_partition=bool(
                    result_CanAssumeNoDupesPerPartition),
            )
            yield result
