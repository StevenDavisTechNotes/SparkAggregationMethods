#!python
# usage: python -m src.generate_data_files [--overwrite | --no-overwrite] [--minimal | --no-minimal]

import argparse
import logging
from dataclasses import dataclass

from spark_agg_methods_common_python.utils.platform import setup_logging
from src.challenges.deduplication.dedupe_generate_data_files import (
    dedupe_derive_source_test_data_remove, dedupe_generate_data_files,
)
from src.challenges.sectional.section_generate_data_files import (
    sectional_generate_data_files, sectional_generate_data_files_remove,
)
from src.challenges.six_field_test_data.six_generate_data_files import (
    six_generate_data_files, six_generate_data_files_remove,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Arguments:
    overwrite: bool
    minimal: bool


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser(description="Generate the test data files for every challenge.")
    parser.add_argument(
        "--overwrite", default=False, action=argparse.BooleanOptionalAction,
        help="delete and regenerate existing data files (default: keep existing files)",
    )
    parser.add_argument(
        "--minimal", default=False, action=argparse.BooleanOptionalAction,
        help="generate only the smallest data sizes (default: minimal)",
    )
    args = parser.parse_args()
    return Arguments(
        overwrite=args.overwrite,
        minimal=args.minimal,
    )


def main(
        args: Arguments,
) -> None:
    overwrite = args.overwrite
    minimal = args.minimal
    logger.info(f"Running {__file__}")
    try:
        if overwrite:
            dedupe_derive_source_test_data_remove()
            sectional_generate_data_files_remove()
            six_generate_data_files_remove()
        dedupe_generate_data_files(
            overwrite=overwrite,
            minimal=minimal,
        )
        sectional_generate_data_files(
            overwrite=overwrite,
            minimal=minimal,
        )
        six_generate_data_files(
            overwrite=overwrite,
            minimal=minimal,
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main(parse_args())
