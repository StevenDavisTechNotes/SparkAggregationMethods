import hashlib
import os

from spark_agg_methods_common_python.challenges.deduplication.dedupe_test_data_types import \
    DedupeDataSetDescription
from spark_agg_methods_common_python.utils.utils import always_true

MAX_EXPONENT = 5
DATA_SIZE_LIST_DEDUPE = [
    DedupeDataSetDescription(
        num_people=num_people,
        num_b_recs=num_b_recs,
        num_sources=num_sources,
    )
    for num_people in [10**x for x in range(0, MAX_EXPONENT + 1)]
    for num_sources in [2, 3, 6]
    if always_true(num_b_recs := max(1, 2 * num_people // 100))
]


def derive_file_path(
        root_path: str,
        source_code: str,
        num_people: int,
) -> str:
    return root_path + "/Dedupe_Field%s%d.csv" % (source_code, num_people)


def name_hash(
        i: int,
) -> str:
    return hashlib.sha512(str(i).encode('utf8')).hexdigest()


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


def generate_data_files(
        root_path: str,
        source_codes: list[str],
        num_people: int,
) -> None:
    for source_code in source_codes:
        file_path = derive_file_path(root_path, source_code, num_people)
        if not os.path.isfile(file_path):
            num_people_to_represent = (
                num_people
                if source_code != 'B' else
                max(1, 2 * num_people // 100)
            )
            with open(file_path, "w") as f:
                for i in range(1, num_people_to_represent + 1):
                    f.write(line(i, source_code))
