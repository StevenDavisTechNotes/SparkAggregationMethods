#!python
# usage: .\venv\Scripts\activate.ps1; python -m src.perf_test_runner_for_pyspark

import logging
import sys

from src.challenges.bi_level import bi_level_runner_pyspark
from src.challenges.conditional import conditional_runner_pyspark
from src.challenges.deduplication import dedupe_runner_pyspark
from src.challenges.sectional import section_runner_pyspark
from src.challenges.vanilla import vanilla_runner_pyspark

logger = logging.getLogger(__name__)


def main():
    logger.info(f"Running {__file__}")
    try:
        vanilla_runner_pyspark.main()
        bi_level_runner_pyspark.main()
        conditional_runner_pyspark.main()
        section_runner_pyspark.main()
        dedupe_runner_pyspark.main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    main()
