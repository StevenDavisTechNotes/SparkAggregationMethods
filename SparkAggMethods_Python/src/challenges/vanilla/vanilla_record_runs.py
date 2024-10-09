import datetime
import os
from dataclasses import dataclass

from src.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase, PersistedRunResultLog, RunResultBase,
    RunResultFileWriterBase, SolutionLanguage,
)
from src.utils.utils import root_folder_abs_path

PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/vanilla_dask_runs.csv'
PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/vanilla_pyspark_runs.csv'
PYTHON_ONLY_RUN_LOG_FILE_PATH = 'results/vanilla_python_only_runs.csv'
SCALA_RUN_LOG_FILE_PATH = '../results/Scala/vanilla_runs_scala.csv'
FINAL_REPORT_FILE_PATH = 'results/vanilla_results.csv'
EXPECTED_SIZES = [3 * 3 * 10**x for x in range(0, 6 + 1)]


@dataclass(frozen=True)
class VanillaRunResult(RunResultBase):
    # engine: CalcEngine
    # data_size: int
    # elapsed_time: float
    # record_count: int
    pass


@dataclass(frozen=True)
class VanillaPersistedRunResult(PersistedRunResultBase):
    # strategy_name: str
    # language: str
    # engine: CalcEngine
    # strategy_w_language_name: str
    # interface: str
    # data_size: int
    # elapsed_time: float
    # record_count: int
    pass


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
            run_log = SCALA_RUN_LOG_FILE_PATH
        case _:
            raise ValueError(f"Unknown engine: {engine}")
    return os.path.join(
        root_folder_abs_path(),
        run_log)


class VanillaPersistedRunResultLog(PersistedRunResultLog):
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
        assert isinstance(result, VanillaPersistedRunResult)
        return result.record_count == 9

    def read_regular_line_from_log_file(
            self,
            line: str,
            fields: list[str],
    ) -> PersistedRunResultBase | None:
        strategy_name, interface, result_data_size, \
            result_elapsed_time, result_record_count, \
            result_finished_at, result_engine  \
            = tuple(fields)
        result = VanillaPersistedRunResult(
            strategy_name=strategy_name,
            language=self.language,
            engine=self.engine,
            strategy_w_language_name=f"{strategy_name}_{self.language}",
            interface=interface,
            num_data_points=int(result_data_size),
            elapsed_time=float(result_elapsed_time),
            record_count=int(result_record_count))
        return result

    def read_scala_line_from_log_file(
            self,
            line: str,
            fields: list[str],
    ) -> PersistedRunResultBase | None:
        outcome, strategy_name, interface, expected_size, returnedSize, elapsed_time = tuple(
            fields)
        if outcome != 'success':
            print("Excluding line: " + line)
            return None
        if returnedSize != '9':
            print("Excluding line: " + line)
            return None
        result = VanillaPersistedRunResult(
            strategy_name=strategy_name,
            engine=self.engine,
            language=self.language,
            strategy_w_language_name=f"{strategy_name}_{self.language}",
            interface=interface,
            num_data_points=int(expected_size),
            elapsed_time=float(elapsed_time),
            record_count=-1,
        )
        return result

    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> PersistedRunResultBase | None:
        match self.engine:
            case CalcEngine.DASK | CalcEngine.PYSPARK | CalcEngine.PYTHON_ONLY:
                return self.read_regular_line_from_log_file(line, fields)
            case CalcEngine.SCALA_SPARK:
                return self.read_scala_line_from_log_file(line, fields)


class VanillaPythonRunResultFileWriter(RunResultFileWriterBase):

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

    def __enter__(self) -> 'VanillaPythonRunResultFileWriter':
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
        print(' strategy,interface,num_data_points,elapsed_time,record_count,engine,finished_at,', file=self.file)
        self.file.flush()

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            result: RunResultBase,
    ) -> None:
        assert isinstance(result, VanillaRunResult)
        assert self.engine == challenge_method_registration.engine
        assert challenge_method_registration.engine == result.engine
        print("%s,%s,%d,%f,%d,%s,%s," % (
            challenge_method_registration.strategy_name,
            challenge_method_registration.interface_getter,
            result.num_data_points, result.elapsed_time, result.record_count,
            challenge_method_registration.engine.value,
            datetime.datetime.now().isoformat()
        ), file=self.file)
        self.file.flush()
