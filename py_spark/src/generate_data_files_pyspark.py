#!python
# usage: ..\.venv\Scripts\activate.ps1 ; python -m src.generate_data_files_pyspark
#        [--overwrite | --no-overwrite] [--minimal | --no-minimal]

import argparse
import logging
from dataclasses import dataclass

from spark_agg_methods_common_python.utils.platform import setup_logging
from src.challenges.six_field_test_data.six_generate_data_files_pyspark import six_generate_data_files_pyspark
from src.utils.tidy_session_pyspark import TidySparkSession

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Arguments:
    overwrite: bool
    minimal: bool


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser(description="Generate the PySpark-format test data files.")
    parser.add_argument(
        "--overwrite", default=False, action=argparse.BooleanOptionalAction,
        help="delete and regenerate existing data files (default: keep existing files)",
    )
    parser.add_argument(
        "--minimal", default=False, action=argparse.BooleanOptionalAction,
        help="generate only the smallest data sizes (default: all sizes)",
    )
    args = parser.parse_args()
    return Arguments(
        overwrite=args.overwrite,
        minimal=args.minimal,
    )


def main(
        args: Arguments,
) -> None:
    logger.info(f"Running {__file__}")
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
        # dedupe_generate_data_files(overwrite=args.overwrite, minimal=args.minimal) TODO: Implement this
        # sectional_generate_data_files(overwrite=args.overwrite, minimal=args.minimal) TODO: Implement this
        six_generate_data_files_pyspark(
            overwrite=args.overwrite,
            minimal=args.minimal,
            spark_session=spark_session,
        )
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main(parse_args())
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
