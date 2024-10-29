import os
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase,
    PersistedRunResultLog, RunResultBase, RunResultFileWriterBase,
    SolutionInterfacePython, SolutionLanguage, parse_interface_python)


@dataclass(frozen=True)
class DedupeRunResult(RunResultBase):
    status: str
    num_sources: int
    num_people_actual: int
    data_size_exponent: int
    num_people_found: int
    in_cloud_mode: bool
    can_assume_no_duplicates_per_partition: bool


@dataclass(frozen=True)
class DedupePersistedRunResult(PersistedRunResultBase[SolutionInterfacePython], DedupeRunResult):
    pass


def regressor_from_run_result(
        result: DedupePersistedRunResult,
) -> int:
    return result.num_source_rows


class DedupePersistedRunResultLog(PersistedRunResultLog[DedupePersistedRunResult]):

    def __init__(
            self,
            engine: CalcEngine,
            language: SolutionLanguage,
            rel_log_file_path: str,
    ):
        super().__init__(
            engine=engine,
            language=language,
            log_file_path=os.path.abspath(rel_log_file_path),
        )

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
            rel_log_file_path: str,
    ):
        super().__init__(
            log_file_path=os.path.abspath(rel_log_file_path),
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
