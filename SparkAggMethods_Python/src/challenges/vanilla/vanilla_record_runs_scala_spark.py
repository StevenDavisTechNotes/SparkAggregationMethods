from spark_agg_methods_common_python.perf_test_common import CalcEngine

from src.challenges.vanilla.vanilla_record_runs import VanillaScalaPersistedRunResultLog

CONDITIONAL_PYTHON_PYSPARK_RUN_LOG_FILE_PATH = 'results/conditional_pyspark_runs.csv'

ENGINE = CalcEngine.SCALA_SPARK


class VanillaScalaSparkPersistedRunResultLog(VanillaScalaPersistedRunResultLog):
    def __init__(self):
        super().__init__(
            engine=ENGINE,
            rel_log_file_path=CONDITIONAL_PYTHON_PYSPARK_RUN_LOG_FILE_PATH,
        )
