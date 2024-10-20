from spark_agg_methods_common_python.challenges.vanilla.vanilla_record_runs import (
    VanillaPythonPersistedRunResultLog, VanillaPythonRunResultFileWriter,
)
from spark_agg_methods_common_python.perf_test_common import CalcEngine, SolutionLanguage

VANILLA_PYSPARK_RUN_LOG_FILE_PATH = 'results/vanilla_pyspark_runs.csv'
LANGUAGE = SolutionLanguage.PYTHON
ENGINE = CalcEngine.PYSPARK


class VanillaPysparkPersistedRunResultLog(VanillaPythonPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=VANILLA_PYSPARK_RUN_LOG_FILE_PATH,
        )


class VanillaPysparkRunResultFileWriter(VanillaPythonRunResultFileWriter):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=VANILLA_PYSPARK_RUN_LOG_FILE_PATH,
        )
