import datetime
from typing import TextIO

from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonDaskRegistration,
    ChallengeMethodPythonOnlyRegistration,
    ChallengeMethodPythonPysparkRegistration)
from six_field_test_data.six_test_data_types import RunResult


def write_header(
        file: TextIO,
) -> None:
    print(' strategy,interface,dataSize,elapsedTime,recordCount,finishedAt,engine,', file=file)
    file.flush()


def write_run_result(
        challenge_method_registration:
        ChallengeMethodPythonOnlyRegistration
        | ChallengeMethodPythonPysparkRegistration | ChallengeMethodPythonDaskRegistration,
        result: RunResult,
        file: TextIO,
) -> None:
    match challenge_method_registration:
        case ChallengeMethodPythonPysparkRegistration():
            engine = CalcEngine.PYSPARK
        case ChallengeMethodPythonDaskRegistration():
            engine = CalcEngine.DASK
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
