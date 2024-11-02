import os

from spark_agg_methods_common_python.challenges.conditional.conditional_record_runs import (
    ConditionalPythonRunResultFileWriter,
)
from spark_agg_methods_common_python.perf_test_common import CalcEngine

ENGINE = CalcEngine.PYTHON_ONLY


class ConditionalPythonOnlyRunResultFileWriter(ConditionalPythonRunResultFileWriter):
    RUN_LOG_FILE_PATH: str = os.path.abspath('results/conditional_python_only_runs.csv')

    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=__class__.RUN_LOG_FILE_PATH,
        )
