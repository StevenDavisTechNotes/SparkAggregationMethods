#!python
# usage: python -m src.generate_data_files

import logging

from spark_agg_methods_common_python.utils.platform import setup_logging

from src.challenges.deduplication.dedupe_generate_data_files import (
    dedupe_generate_data_files,
)
from src.challenges.sectional.section_generate_data_files import (
    sectional_generate_data_files,
)
from src.challenges.six_field_test_data.six_generate_data_files import (
    six_generate_data_files,
)

logger = logging.getLogger(__name__)


MAKE_NEW_FILES: bool = False


def main(
        *,
        make_new_files: bool,
) -> None:
    logger.info(f"Running {__file__}")
    try:
        dedupe_generate_data_files(make_new_files=make_new_files)
        sectional_generate_data_files(make_new_files=make_new_files)
        six_generate_data_files(make_new_files=make_new_files)
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main(make_new_files=MAKE_NEW_FILES)
