from spark_agg_methods_common_python.perf_test_common import CalcEngine

from src.challenges.vanilla.vanilla_record_runs import (
    VanillaPythonPersistedRunResultLog, VanillaPythonRunResultFileWriter,
)

VANILLA_PYTHON_ONLY_RUN_LOG_FILE_PATH = 'results/vanilla_python_only_runs.csv'
ENGINE = CalcEngine.PYTHON_ONLY


class VanillaPythonOnlyPersistedRunResultLog(VanillaPythonPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=VANILLA_PYTHON_ONLY_RUN_LOG_FILE_PATH,
        )


class VanillaPythonOnlyRunResultFileWriter(VanillaPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=VANILLA_PYTHON_ONLY_RUN_LOG_FILE_PATH,
        )
