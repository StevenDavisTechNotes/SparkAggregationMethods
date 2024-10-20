from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

from src.challenges.vanilla.vanilla_record_runs import (
    VanillaPythonPersistedRunResultLog, VanillaPythonRunResultFileWriter,
)

VANILLA_DASK_RUN_LOG_FILE_PATH = 'results/vanilla_dask_runs.csv'
LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.DASK


class VanillaDaskPersistedRunResultLog(VanillaPythonPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=VANILLA_DASK_RUN_LOG_FILE_PATH,
        )


class VanillaDaskRunResultFileWriter(VanillaPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=VANILLA_DASK_RUN_LOG_FILE_PATH,
        )
