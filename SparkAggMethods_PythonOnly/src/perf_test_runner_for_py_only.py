#!python
# usage: python -m src.perf_test_runner_for_py_only

import logging
import sys

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
        # dedupe_python_only_runner.main()
        section_runner_py_only.main()
        vanilla_runner_py_only.main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    main()
