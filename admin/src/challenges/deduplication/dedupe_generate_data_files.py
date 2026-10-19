import logging
import shutil
from pathlib import Path

from spark_agg_methods_common_python.challenges.deduplication.dedupe_test_data_types import (
    DATA_SIZE_LIST_DEDUPE_MAXIMAL, DATA_SIZE_LIST_DEDUPE_MINIMAL, DEDUPE_SOURCE_CODES,
    dedupe_derive_source_test_data_file_path_root, dedupe_derive_source_test_data_file_paths, name_hash,
)
from spark_agg_methods_common_python.utils.platform import setup_logging

logger = logging.getLogger(__name__)


def line(
        i: int,
        misspelledLetter: str,
) -> str:
    letter = misspelledLetter
    v = f"""
FFFFFF{letter}{i}_{name_hash(i)},
LLLLLL{letter}{i}_{name_hash(i)},
{i} Main St,Plaineville ME,
{(i-1) % 100:05d},
{i},
{i*2 if letter == "A" else ""},
{i*3 if letter == "B" else ""},
{i*5 if letter == "C" else ""},
{i*7 if letter == "D" else ""},
{i*11 if letter == "E" else ""},
{i*13 if letter == "F" else ""}
"""  # cSpell: ignore Plaineville
    v = v.replace("\n", "") + "\n"
    return v


def dedupe_generate_data_file(
        temp_source_data_file_path: Path,
        source_data_file_path: Path,
        source_code: str,
        num_people: int,
) -> None:
    source_data_file_path.unlink(missing_ok=True)
    temp_source_data_file_path.unlink(missing_ok=True)
    logger.info(f"Creating {source_data_file_path}")
    num_people_to_represent = (
        num_people
        if source_code != "B" else
        max(1, 2 * num_people // 100)
    )
    with temp_source_data_file_path.open("w") as f:
        for i in range(1, num_people_to_represent + 1):
            f.write(line(i, source_code))
    temp_source_data_file_path.rename(source_data_file_path)


def dedupe_derive_source_test_data_remove():
    root_path = dedupe_derive_source_test_data_file_path_root()
    if root_path.exists():
        shutil.rmtree(root_path)


def dedupe_generate_data_files(
        *,
        overwrite: bool = False,
        minimal: bool = False,
) -> None:
    data_descriptions = (
        DATA_SIZE_LIST_DEDUPE_MINIMAL
        if minimal else
        DATA_SIZE_LIST_DEDUPE_MAXIMAL
    )
    for data_description in data_descriptions:
        temp_source_data_file_paths = dedupe_derive_source_test_data_file_paths(
            data_description=data_description,
            temp_file=True,
        )
        source_data_file_paths = dedupe_derive_source_test_data_file_paths(
            data_description=data_description,
            temp_file=False,
        )
        for source_code in DEDUPE_SOURCE_CODES:
            temp_source_data_file_path = temp_source_data_file_paths[source_code]
            source_data_file_path = source_data_file_paths[source_code]
            if overwrite is True or not source_data_file_path.exists():
                source_data_file_path.parent.mkdir(parents=True, exist_ok=True)
                dedupe_generate_data_file(
                    temp_source_data_file_path=temp_source_data_file_path,
                    source_data_file_path=source_data_file_path,
                    source_code=source_code,
                    num_people=data_description.num_people,
                )


def main():
    logger.info(f"Running {__file__}")
    dedupe_generate_data_files()
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
