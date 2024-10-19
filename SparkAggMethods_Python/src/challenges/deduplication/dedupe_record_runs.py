import os
from dataclasses import dataclass

from spark_agg_methods_common_python.utils.utils import always_true

from src.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase, PersistedRunResultLog, RunResultBase,
    RunResultFileWriterBase, SolutionInterfacePython, SolutionLanguage, parse_interface_python,
)

PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/dedupe_pyspark_runs.csv'
PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/dedupe_dask_runs.csv'
FINAL_REPORT_FILE_PATH = 'results/dedupe_results.csv'
LANGUAGE = SolutionLanguage.PYTHON


@dataclass(frozen=True)
class DedupeRunResult(RunResultBase):
    # for RunResultBase
    num_source_rows: int
    elapsed_time: float
    num_output_rows: int
    finished_at: str | None
    # for DedupeRunResult
    status: str
    num_sources: int
    num_people_actual: int
    data_size_exponent: int
    num_people_found: int
    in_cloud_mode: bool
    can_assume_no_duplicates_per_partition: bool


@dataclass(frozen=True)
class DedupePersistedRunResult(PersistedRunResultBase[SolutionInterfacePython], DedupeRunResult):
    # for RunResultBase
    num_source_rows: int
    elapsed_time: float
    num_output_rows: int
    finished_at: str | None
    # for PersistedRunResultBase
    language: SolutionLanguage
    engine: CalcEngine
    interface: SolutionInterfacePython
    strategy_name: str
    # for DedupeRunResult
    status: str
    num_sources: int
    num_people_actual: int
    data_size_exponent: int
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


def derive_run_log_file_path(
        engine: CalcEngine,
) -> str:
    match engine:
        case  CalcEngine.PYSPARK:
            run_log = PYTHON_PYSPARK_RUN_LOG_FILE_PATH
        case CalcEngine.DASK:
            run_log = PYTHON_DASK_RUN_LOG_FILE_PATH
        case _:
            raise ValueError(f"Unknown engine: {engine}")
    return os.path.abspath(run_log)


def regressor_from_run_result(
        result: DedupePersistedRunResult,
) -> int:
    return result.num_source_rows


class DedupePersistedRunResultLog(PersistedRunResultLog[DedupePersistedRunResult]):
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

    def derive_run_log_file_path(
            self,
    ) -> str | None:
        return derive_run_log_file_path(self.engine)

    def result_looks_valid(
            self,
            result: DedupePersistedRunResult,
    ) -> bool:
        assert isinstance(result, DedupePersistedRunResult)
        return result.status == "success"

    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> DedupePersistedRunResult | None:
        if len(fields) < 6:
            fields.append('3')
        fields_as_dict = dict(zip(last_header_line, fields))
        status = fields_as_dict['status']
        strategy_name = fields_as_dict['strategy_name']
        interface = fields_as_dict['interface']
        num_sources = int(fields_as_dict['num_sources'])
        num_source_rows = int(fields_as_dict['num_source_rows'])
        data_size_exponent = int(fields_as_dict['data_size_exponent'])
        num_people_actual = int(fields_as_dict['num_people_actual'])
        elapsed_time = float(fields_as_dict['elapsed_time'])
        num_people_found = int(fields_as_dict['num_people_found'])
        in_cloud_mode = bool(fields_as_dict['in_cloud_mode'])
        can_assume_no_duplicates_per_partition = bool(fields_as_dict['can_assume_no_duplicates_per_partition'])
        engine = fields_as_dict['engine']
        finished_at = fields_as_dict['finished_at']

        assert engine == self.engine.value
        return DedupePersistedRunResult(
            num_source_rows=num_source_rows,
            elapsed_time=elapsed_time,
            num_output_rows=num_people_actual,
            finished_at=finished_at,

            language=self.language,
            engine=self.engine,
            interface=parse_interface_python(interface, self.engine),
            strategy_name=strategy_name,

            status=status,
            num_sources=num_sources,
            num_people_actual=num_people_actual,
            data_size_exponent=data_size_exponent,
            num_people_found=num_people_found,
            in_cloud_mode=in_cloud_mode,
            can_assume_no_duplicates_per_partition=can_assume_no_duplicates_per_partition,
        )


class DedupePythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            engine: CalcEngine,
    ):
        super().__init__(
            file_name=derive_run_log_file_path(engine),
            language=SolutionLanguage.PYTHON,
            engine=engine,
            persisted_row_type=DedupePersistedRunResult,
        )

    def __enter__(self) -> 'DedupePythonRunResultFileWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            run_result: RunResultBase,
    ) -> None:
        assert isinstance(run_result, DedupeRunResult)
        assert self.engine == challenge_method_registration.engine
        run_result = DedupePersistedRunResult(
            num_source_rows=run_result.num_source_rows,
            elapsed_time=run_result.elapsed_time,
            num_output_rows=run_result.num_output_rows,
            finished_at=run_result.finished_at,

            language=self.language,
            engine=self.engine,
            interface=challenge_method_registration.interface,
            strategy_name=challenge_method_registration.strategy_name,

            status=run_result.status,
            num_sources=run_result.num_sources,
            num_people_actual=run_result.num_people_actual,
            data_size_exponent=run_result.data_size_exponent,
            num_people_found=run_result.num_people_found,
            in_cloud_mode=run_result.in_cloud_mode,
            can_assume_no_duplicates_per_partition=run_result.can_assume_no_duplicates_per_partition,
        )
        self.file.flush()
