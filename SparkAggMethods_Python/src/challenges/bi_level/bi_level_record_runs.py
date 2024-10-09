import datetime
import os
from dataclasses import dataclass

from src.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase, PersistedRunResultLog, RunResultBase,
    RunResultFileWriterBase, SolutionLanguage,
)
from src.utils.utils import root_folder_abs_path

PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/bi_level_dask_runs.csv'
PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/bi_level_pyspark_runs.csv'
PYTHON_ONLY_RUN_LOG_FILE_PATH = 'results/bi_level_python_only_runs.csv'
FINAL_REPORT_FILE_PATH = 'results/bilevel_results.csv'
EXPECTED_SIZES = [1, 10, 100, 1000]


@dataclass(frozen=True)
class BiLevelRunResult(RunResultBase):
    # engine: CalcEngine
    # data_size: int
    relative_cardinality: int
    # elapsedTime: float
    # record_count: int


@dataclass(frozen=True)
class BiLevelPersistedRunResult(PersistedRunResultBase):
    # strategy_name: str
    # language: str
    # engine: CalcEngine
    # interface: str
    # data_size: int
    relative_cardinality: int
    # elapsed_time: float
    # record_count: int


def derive_run_log_file_path_for_recording(
        engine: CalcEngine,
) -> str:
    match engine:
        case CalcEngine.DASK:
            run_log = PYTHON_DASK_RUN_LOG_FILE_PATH
        case CalcEngine.PYSPARK:
            run_log = PYTHON_PYSPARK_RUN_LOG_FILE_PATH
        case CalcEngine.PYTHON_ONLY:
            run_log = PYTHON_ONLY_RUN_LOG_FILE_PATH
        case CalcEngine.SCALA_SPARK:
            assert False, "Scala engine not supported in Python"
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
        result: PersistedRunResultBase,
) -> int:
    assert isinstance(result, BiLevelPersistedRunResult)
    return result.relative_cardinality


class BiLevelPersistedRunResultLog(PersistedRunResultLog):
    def __init__(
            self,
            engine: CalcEngine,
    ):
        self.engine = engine
        match engine:
            case CalcEngine.DASK | CalcEngine.PYSPARK | CalcEngine.PYTHON_ONLY:
                language = SolutionLanguage.PYTHON
            case CalcEngine.SCALA_SPARK:
                language = SolutionLanguage.SCALA
            case _:
                raise ValueError(f"Unknown engine: {engine}")
        super().__init__(
            engine=engine,
            language=language,
        )

    def derive_run_log_file_path_for_reading(
            self,
    ) -> str | None:
        match self.engine:
            case CalcEngine.DASK | CalcEngine.PYSPARK | CalcEngine.PYTHON_ONLY:
                return os.path.join(
                    root_folder_abs_path(),
                    derive_run_log_file_path_for_recording(self.engine),
                )
            case CalcEngine.SCALA_SPARK:
                return None
            case _:
                raise ValueError(f"Unknown engine: {self.engine}")

    def result_looks_valid(
            self,
            result: PersistedRunResultBase,
    ) -> bool:
        assert isinstance(result, BiLevelPersistedRunResult)
        return result.record_count == 3

    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> PersistedRunResultBase | None:
        if len(fields) < 6:
            fields.append('3')
        fields_as_dict = dict(zip(last_header_line, fields))
        # strategy_name, interface, num_data_points, relative_cardinality, elapsed_time, record_count, *rest = fields
        strategy_name = fields_as_dict['strategy']
        interface = fields_as_dict['interface']
        num_data_points = int(fields_as_dict['num_data_points'])
        relative_cardinality = int(fields_as_dict['relative_cardinality'])
        elapsed_time = float(fields_as_dict['elapsed_time'])
        record_count = int(fields_as_dict['record_count']) if 'record_count' in fields_as_dict else -1
        return BiLevelPersistedRunResult(
            strategy_name=strategy_name,
            language=self.language,
            engine=self.engine,
            strategy_w_language_name=f"{strategy_name}_{self.language}",
            interface=interface,
            num_data_points=num_data_points,
            relative_cardinality=relative_cardinality,
            elapsed_time=elapsed_time,
            record_count=record_count,
        )


class BiLevelPythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            engine: CalcEngine,
    ):
        match engine:
            case CalcEngine.DASK | CalcEngine.PYSPARK | CalcEngine.PYTHON_ONLY:
                language = SolutionLanguage.PYTHON
            case CalcEngine.SCALA_SPARK:
                raise ValueError("Scala has its own writer")
            case _:
                raise ValueError(f"Unknown engine: {engine}")
        self.engine = engine
        self.language = language
        super().__init__(
            file_name=__class__.derive_run_log_file_path_for_recording(engine),
            engine=engine,
            language=language,
        )

    def __enter__(self) -> 'BiLevelPythonRunResultFileWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def derive_run_log_file_path_for_recording(
            engine: CalcEngine,
    ) -> str:
        return os.path.join(
            root_folder_abs_path(),
            derive_run_log_file_path_for_recording(engine),
        )

    def write_header(
            self,
    ) -> None:
        print(' strategy,interface,num_data_points,relative_cardinality,elapsed_time,record_count,engine,finished_at,',
              file=self.file)
        self.file.flush()

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            result: RunResultBase,
    ) -> None:
        assert isinstance(result, BiLevelRunResult)
        assert self.engine == challenge_method_registration.engine
        assert challenge_method_registration.engine == result.engine
        print("%s,%s,%d,%d,%f,%d,%s,%s," % (
            challenge_method_registration.strategy_name,
            challenge_method_registration.interface_getter,
            result.num_data_points,
            result.relative_cardinality,
            result.elapsed_time,
            result.record_count,
            challenge_method_registration.engine.value,
            datetime.datetime.now().isoformat(),
        ), file=self.file)
        self.file.flush()
