#!python
# usage: python -m src.generate_data

from src.challenges.bi_level.bi_level_generate_data import bi_level_generate_data_files
from src.challenges.conditional.conditional_generate_data import conditional_generate_data_files
from src.challenges.deduplication.dedupe_generate_data import dedupe_generate_data_files
from src.challenges.sectional.section_generate_data import sectional_generate_data
from src.challenges.vanilla.vanilla_generate_data import vanilla_generate_data_files

MAKE_NEW_FILES: bool = False


def main(
        *,
        make_new_files: bool,
) -> None:
    print(f"Running {__file__}")
    try:
        bi_level_generate_data_files(make_new_files=make_new_files)
        conditional_generate_data_files(make_new_files=make_new_files)
        dedupe_generate_data_files(make_new_files=make_new_files)
        sectional_generate_data(make_new_files=make_new_files)
        vanilla_generate_data_files(make_new_files=make_new_files)
    except KeyboardInterrupt:
        print("Interrupted!")
        return
    print("Done!")


if __name__ == "__main__":
    main(make_new_files=MAKE_NEW_FILES)
