from spark_agg_methods_common_python.challenges.bi_level.bi_level_record_runs import (
    BiLevelPersistedRunResultLog, BiLevelPythonRunResultFileWriter,
)
from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

BI_LEVEL_PYTHON_DASK_RUN_LOG_FILE_PATH = 'results/bi_level_dask_runs.csv'

LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.DASK


class BiLevelDaskPersistedRunResultLog(BiLevelPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            language=LANGUAGE,
            rel_log_file_path=BI_LEVEL_PYTHON_DASK_RUN_LOG_FILE_PATH,
        )


class BiLevelDaskRunResultFileWriter(BiLevelPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=BI_LEVEL_PYTHON_DASK_RUN_LOG_FILE_PATH,
        )
