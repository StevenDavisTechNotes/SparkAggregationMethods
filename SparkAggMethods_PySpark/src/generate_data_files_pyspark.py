#!python
# usage: .\venv\Scripts\Activate.ps1 ; python -m src.generate_data_files_pyspark

import logging

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.six_field_test_data.six_generate_data_files_pyspark import six_generate_data_files_pyspark
from src.utils.tidy_session_pyspark import TidySparkSession

logger = logging.getLogger(__name__)


MAKE_NEW_FILES: bool = True


def main(
        *,
        make_new_files: bool,
) -> None:
    logger.info(f"Running {__file__}")
    try:
        config = {
            "spark.sql.shuffle.partitions": 1,
            "spark.default.parallelism": 1,
            "spark.driver.memory": "2g",
            "spark.executor.memory": "3g",
            "spark.executor.memoryOverhead": "1g",
        }
        with TidySparkSession(
            config,
            enable_hive_support=False
        ) as spark_session:
            # dedupe_generate_data_files(make_new_files=make_new_files) TODO: Implement this
            # sectional_generate_data_files(make_new_files=make_new_files) TODO: Implement this
            six_generate_data_files_pyspark(
                make_new_files=make_new_files,
                spark_session=spark_session,
            )
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main(make_new_files=MAKE_NEW_FILES)
