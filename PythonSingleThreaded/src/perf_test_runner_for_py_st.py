#!python
# usage: .\venv\Scripts\activate.ps1; python -O -m src.perf_test_runner_for_py_st

import logging

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.bi_level import bi_level_runner_py_st
from src.challenges.conditional import conditional_runner_py_st
# from src.challenges.deduplication import dedupe_runner_py_st
from src.challenges.sectional import section_runner_py_st
from src.challenges.vanilla import vanilla_runner_py_st

logger = logging.getLogger(__name__)


def main():
    logger.info(f"Running {__file__}")
    try:
        bi_level_runner_py_st.main()
        conditional_runner_py_st.main()
        # dedupe_runner_py_st.main()  TODO: Implement this
        section_runner_py_st.main()
        vanilla_runner_py_st.main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main()
