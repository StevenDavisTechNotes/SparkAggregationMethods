#!python
# usage: .\venv\Scripts\activate.ps1; python -O -m src.perf_test_runner_for_pyspark

import logging

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.bi_level import bi_level_runner_pyspark
from src.challenges.conditional import conditional_runner_pyspark
from src.challenges.deduplication import dedupe_runner_pyspark
from src.challenges.sectional import section_runner_pyspark
from src.challenges.vanilla import vanilla_runner_pyspark

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info(f"Running {__file__}")
    vanilla_runner_pyspark.main()
    bi_level_runner_pyspark.main()
    conditional_runner_pyspark.main()
    section_runner_pyspark.main()
    dedupe_runner_pyspark.main()
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
