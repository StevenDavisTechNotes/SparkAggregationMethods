import os
from dataclasses import dataclass

from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, ChallengeMethodRegistrationBase, PersistedRunResultBase,
    PersistedRunResultLog, RunResultBase, RunResultFileWriterBase,
    SolutionInterface, SolutionLanguage, parse_interface_python)


@dataclass(frozen=True)
class SectionRunResult(RunResultBase):
    status: str
    section_maximum: int


@dataclass(frozen=True)
class SectionPersistedRunResult(PersistedRunResultBase[SolutionInterface], SectionRunResult):
    num_students: int


def regressor_from_run_result(
        result: SectionPersistedRunResult,
) -> int:
    return result.num_students


class SectionPythonPersistedRunResultLog(PersistedRunResultLog[SectionPersistedRunResult]):

    def __init__(
            self,
            engine: CalcEngine,
            rel_log_file_path: str,
    ):
        super().__init__(
            engine=engine,
            language=SolutionLanguage.PYTHON,
            log_file_path=os.path.abspath(rel_log_file_path),
        )

    def result_looks_valid(
            self,
            result: SectionPersistedRunResult,
    ) -> bool:
        assert isinstance(result, SectionPersistedRunResult)
        return result.num_output_rows == 3

    def read_line_from_log_file(
            self,
            i_line: int,
            line: str,
            last_header_line: list[str],
            fields: list[str],
    ) -> SectionPersistedRunResult | None:
        if len(fields) < 6:
            fields.append('3')
        fields_as_dict = dict(zip(last_header_line, fields))

        num_source_rows = int(fields_as_dict['num_source_rows'])
        elapsed_time = float(fields_as_dict['elapsed_time'])
        num_output_rows = int(fields_as_dict['num_output_rows']) if 'num_output_rows' in fields_as_dict else -1
        finished_at = fields_as_dict['finished_at']

        interface = fields_as_dict['interface']
        strategy_name = fields_as_dict['strategy_name']

        status = fields_as_dict['status']
        section_maximum = int(fields_as_dict['section_maximum'])
        num_students = int(fields_as_dict['num_students'])
        return SectionPersistedRunResult(
            num_source_rows=num_source_rows,
            elapsed_time=elapsed_time,
            num_output_rows=num_output_rows,
            finished_at=finished_at,

            language=self.language,
            engine=self.engine,
            interface=parse_interface_python(interface, self.engine),
            strategy_name=strategy_name,

            status=status,
            section_maximum=section_maximum,
            num_students=num_students,
        )


class SectionPythonRunResultFileWriter(RunResultFileWriterBase):

    def __init__(
            self,
            engine: CalcEngine,
            rel_log_file_path: str,
    ):
        super().__init__(
            log_file_path=os.path.abspath(rel_log_file_path),
            language=SolutionLanguage.PYTHON,
            engine=engine,
            persisted_row_type=SectionPersistedRunResult,
        )

    def __enter__(self) -> 'SectionPythonRunResultFileWriter':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def write_run_result(
            self,
            challenge_method_registration: ChallengeMethodRegistrationBase,
            run_result: RunResultBase,
    ) -> None:
        assert isinstance(run_result, SectionRunResult)
        assert self.engine == challenge_method_registration.engine
        run_result = SectionPersistedRunResult(
            num_source_rows=run_result.num_source_rows,
            elapsed_time=run_result.elapsed_time,
            num_output_rows=run_result.num_output_rows,
            finished_at=run_result.finished_at,

            language=self.language,
            engine=self.engine,
            interface=challenge_method_registration.interface,
            strategy_name=challenge_method_registration.strategy_name,

            status=run_result.status,
            section_maximum=run_result.section_maximum,

            num_students=run_result.num_output_rows,

        )
        self.file.flush()
