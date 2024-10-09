import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Callable, Iterable, TextIO

import pandas as pd


class CalcEngine(StrEnum):
    DASK = 'dask'
    PYSPARK = 'pyspark'
    PYTHON_ONLY = 'python_only'
    SCALA_SPARK = 'sc_spark'


class SolutionLanguage(StrEnum):
    PYTHON = 'python'
    SCALA = 'scala'


class SolutionInterfaceDask(StrEnum):
    DASK_BAG = 'bag'
    DASK_DATAFRAME = 'ddf'
    DASK_SQL = 'sql'


class SolutionInterfacePythonOnly(StrEnum):
    PYTHON = 'python'
    PANDAS = 'pandas'


class SolutionInterfacePySpark(StrEnum):
    PYSPARK_RDD = 'rdd'
    PYSPARK_DATAFRAME_SQL = 'sql'
    PYSPARK_DATAFRAME_FLUENT = 'fluent'
    PYSPARK_DATAFRAME_PANDAS = 'pandas'


class SolutionInterfaceScalaSpark(StrEnum):
    SPARK_RDD = 'rdd'
    SPARK_SQL = 'sql'
    SPARK_DATAFRAME_PANDAS = 'pandas'


SolutionInterface = (
    SolutionInterfaceDask
    | SolutionInterfacePythonOnly
    | SolutionInterfacePySpark
    | SolutionInterfaceScalaSpark
)


@dataclass(frozen=True)
class ChallengeMethodRegistrationBase(ABC):
    strategy_name_2018: str | None
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    # interface: SolutionInterface
    requires_gpu: bool
    # delegate: Callable | None

    @property
    @abstractmethod
    def delegate_getter(self) -> Callable | None:
        pass

    @property
    @abstractmethod
    def interface_getter(self) -> SolutionInterface:
        pass


def count_iter(
        iterator: Iterable
):
    count = 0
    for _ in iterator:
        count += 1
    return count


@dataclass(frozen=True)
class PersistedRunResultBase:
    strategy_name: str
    language: str
    engine: CalcEngine
    strategy_w_language_name: str
    interface: str
    num_data_points: int
    elapsed_time: float
    record_count: int


@dataclass(frozen=True)
class RunResultBase:
    engine: CalcEngine
    num_data_points: int
    # relative_cardinality: int
    elapsed_time: float
    record_count: int


class PersistedRunResultLog(ABC):
    engine: CalcEngine
    language: SolutionLanguage

    def __init__(
            self,
            engine: CalcEngine,
            language: SolutionLanguage,
    ):
        self.engine = engine
        self.language = language

    @abstractmethod
    def derive_run_log_file_path_for_reading(self) -> str | None:
        pass

    @abstractmethod
    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> PersistedRunResultBase | None:
        pass

    @abstractmethod
    def result_looks_valid(
            self,
            result: PersistedRunResultBase,
    ) -> bool:
        pass

    def read_run_result_file(
            self,
    ) -> list[PersistedRunResultBase]:
        log_file_path = self.derive_run_log_file_path_for_reading()
        # language = engine_implied_language(engine)
        if log_file_path is None or not os.path.exists(log_file_path):
            return []
        test_runs: list[PersistedRunResultBase] = []
        last_header_line: list[str] | None = None
        with open(log_file_path, 'r') as f:
            for i_line, line in enumerate(f):
                line = line.rstrip()
                if line.startswith('#'):
                    print("Excluding line: " + line)
                    continue
                if line.startswith(' '):
                    last_header_line = line.strip().split(',')
                    continue
                if line.find(',') < 0:
                    print("Excluding line: " + line)
                    continue
                fields = line.rstrip().split(',')
                if fields[-1] == '':
                    fields = fields[:-1]
                assert last_header_line is not None
                result = self.read_line_from_log_file(i_line+1, line, last_header_line, fields)
                if result is None:
                    print(f"Excluding line {i_line+1}: {line}")
                    continue
                if not self.result_looks_valid(result):
                    print(f"Excluding line {i_line+1}: {line}")
                    continue
                test_runs.append(result)
        return test_runs


class RunResultFileWriterBase(ABC):
    file: TextIO
    engine: CalcEngine
    language: SolutionLanguage

    def __init__(
            self,
            *,
            file_name: str,
            engine: CalcEngine,
            language: SolutionLanguage
    ):
        self.file = open(file_name, mode='at+')
        self.engine = engine
        self.language = language
        self.write_header()

    @abstractmethod
    def write_header(
            self,
    ) -> None:
        pass

    @abstractmethod
    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            result: RunResultBase,
    ) -> None:
        pass

    def close(self):
        self.file.flush()
        self.file.close()
        del self.file


def print_test_runs_summary(
        test_runs_summary: dict[str, dict[int, int]]
):
    df_test_runs_summary = pd.DataFrame.from_records(
        [
            {"strategy_name": strategy_name}
            | {
                "size_%d" % size: test_runs_summary[strategy_name][size]
                for size in sorted(test_runs_summary[strategy_name])
            }
            for strategy_name in sorted(test_runs_summary.keys())
        ], index="strategy_name")
    print(df_test_runs_summary)
