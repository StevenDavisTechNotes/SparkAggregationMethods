#!python
# usage: python -O -m src.perf_test_runner_for_py_only

import logging

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.bi_level import bi_level_runner_py_only
from src.challenges.conditional import conditional_runner_py_only
# from src.challenges.deduplication import dedupe_python_only_runner
from src.challenges.sectional import section_runner_py_only
from src.challenges.vanilla import vanilla_runner_py_only

logger = logging.getLogger(__name__)


def main():
    logger.info(f"Running {__file__}")
    try:
        bi_level_runner_py_only.main()
        conditional_runner_py_only.main()
        # dedupe_python_only_runner.main()  TODO: Implement this
        section_runner_py_only.main()
        vanilla_runner_py_only.main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main()
