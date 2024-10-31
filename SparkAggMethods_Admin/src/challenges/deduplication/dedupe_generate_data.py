import os
from pathlib import Path

from spark_agg_methods_common_python.challenges.deduplication.dedupe_test_data_types import (
    DATA_SIZE_LIST_DEDUPE, DEDUPE_SOURCE_CODES, dedupe_derive_source_test_data_file_paths, name_hash,
)


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
{i*2 if letter == "A" else ''},
{i*3 if letter == "B" else ''},
{i*5 if letter == "C" else ''},
{i*7 if letter == "D" else ''},
{i*11 if letter == "E" else ''},
{i*13 if letter == "F" else ''}
"""  # cSpell: ignore Plaineville
    v = v.replace("\n", "") + "\n"
    return v


def dedupe_generate_data_file(
        temp_source_data_file_path: str,
        source_data_file_path: str,
        source_code: str,
        num_people: int,
) -> None:
    if os.path.exists(source_data_file_path):
        os.unlink(source_data_file_path)
    if os.path.exists(temp_source_data_file_path):
        os.unlink(temp_source_data_file_path)
    num_people_to_represent = (
        num_people
        if source_code != 'B' else
        max(1, 2 * num_people // 100)
    )
    with open(temp_source_data_file_path, "w") as f:
        for i in range(1, num_people_to_represent + 1):
            f.write(line(i, source_code))
    os.rename(temp_source_data_file_path, source_data_file_path)


def dedupe_generate_data_files(
        make_new_files: bool,
) -> None:
    for data_description in DATA_SIZE_LIST_DEDUPE:
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
            if make_new_files is True or os.path.exists(source_data_file_path) is False:
                Path(source_data_file_path).parent.mkdir(parents=True, exist_ok=True)
                dedupe_generate_data_file(
                    temp_source_data_file_path=temp_source_data_file_path,
                    source_data_file_path=source_data_file_path,
                    source_code=source_code,
                    num_people=data_description.num_people,
                )


if __name__ == "__main__":
    print(f"Running {__file__}")
    dedupe_generate_data_files(make_new_files=False)
    print("Done!")
