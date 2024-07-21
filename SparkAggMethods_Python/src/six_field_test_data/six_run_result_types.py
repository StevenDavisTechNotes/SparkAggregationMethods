import datetime
import os
from typing import Callable, TextIO

from src.challenges.vanilla.vanilla_record_runs import (
    PersistedRunResult, derive_run_log_file_path, engine_implied_language)
from src.perf_test_common import CalcEngine
from src.six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonDaskRegistration,
    ChallengeMethodPythonOnlyRegistration,
    ChallengeMethodPythonPysparkRegistration,
    ChallengeMethodPythonRegistration)
from src.six_field_test_data.six_test_data_types import RunResult


def _read_python_file(
        engine: CalcEngine,
        qualifier: Callable[[PersistedRunResult], bool],
) -> list[PersistedRunResult]:
    log_file_path = derive_run_log_file_path(engine)
    language = engine_implied_language(engine)
    if not os.path.exists(log_file_path):
        return []
    test_runs: list[PersistedRunResult] = []
    with open(log_file_path, 'r') as f:
        for i_line, line in enumerate(f):
            line = line.rstrip()
            if line.startswith('#'):
                print("Excluding line: " + line)
                continue
            if line.startswith(' '):
                print("Excluding line: " + line)
                continue
            if line.find(',') < 0:
                print("Excluding line: " + line)
                continue
            fields = line.rstrip().split(',')
            if fields[-1] == '':
                fields = fields[:-1]
            strategy_name, interface, result_data_size, \
                result_elapsed_time, result_record_count, \
                result_finished_at, result_engine  \
                = tuple(fields)
            result = PersistedRunResult(
                strategy_name=strategy_name,
                language=language,
                engine=engine,
                strategy_w_language_name=f"{strategy_name}_{language}",
                interface=interface,
                data_size=int(result_data_size),
                elapsed_time=float(result_elapsed_time),
                record_count=int(result_record_count))
            if not qualifier(result):
                print(f"Excluding line {i_line+1}: {line}")
                continue
            test_runs.append(result)
    return test_runs


def _read_scala_file(
        engine: CalcEngine,
        qualifier: Callable[[PersistedRunResult], bool],
) -> list[PersistedRunResult]:
    log_file_path = derive_run_log_file_path(engine)
    language = engine_implied_language(engine)
    if not os.path.exists(log_file_path):
        return []
    test_runs: list[PersistedRunResult] = []
    with open(log_file_path, 'r') as f:
        for i_line, line in enumerate(f):
            line = line.rstrip()
            if line.startswith('#'):
                print("Excluding line: " + line)
                continue
            if line.startswith(' '):
                print("Excluding line: " + line)
                continue
            if line.find(',') < 0:
                print("Excluding line: " + line)
                continue
            fields = line.rstrip().split(',')
            outcome, strategy_name, interface, expected_size, returnedSize, elapsed_time = tuple(
                fields)
            if outcome != 'success':
                print("Excluding line: " + line)
                continue
            if returnedSize != '9':
                print("Excluding line: " + line)
                continue
            # language = 'scala'
            result = PersistedRunResult(
                strategy_name=strategy_name,
                engine=engine,
                language=language,
                strategy_w_language_name=f"{strategy_name}_{language}",
                interface=interface,
                data_size=int(expected_size),
                elapsed_time=float(elapsed_time),
                record_count=-1)
            if not qualifier(result):
                print(f"Excluding line {i_line+1}: {line}")
                continue
            test_runs.append(result)
    return test_runs


def read_result_file(
        engine: CalcEngine,
        qualifier: Callable[[PersistedRunResult], bool],
) -> list[PersistedRunResult]:
    if engine == CalcEngine.SCALA_SPARK:
        return _read_scala_file(engine, qualifier)
    else:
        return _read_python_file(engine, qualifier)


def write_header(
        file: TextIO,
) -> None:
    print(' strategy,interface,dataSize,elapsedTime,recordCount,finishedAt,engine,', file=file)
    file.flush()


def write_run_result(
        challenge_method_registration: ChallengeMethodPythonRegistration,
        result: RunResult,
        file: TextIO,
) -> None:
    match challenge_method_registration:
        case ChallengeMethodPythonDaskRegistration():
            engine = CalcEngine.DASK
        case ChallengeMethodPythonPysparkRegistration():
            engine = CalcEngine.PYSPARK
        case ChallengeMethodPythonOnlyRegistration():
            engine = CalcEngine.PYTHON_ONLY
        case _:  # pyright: ignore[reportUnnecessaryComparison]
            raise ValueError(f"Unknown challenge_method_registration: {challenge_method_registration}")
    assert engine == result.engine
    print("%s,%s,%d,%f,%d,%s,%s," % (
        challenge_method_registration.strategy_name, challenge_method_registration.interface,
        result.dataSize, result.elapsedTime, result.recordCount,
        engine.value,
        datetime.datetime.now().isoformat()
    ), file=file)
    file.flush()
