from spark_agg_methods_common_python.challenges.bi_level.bi_level_test_data_types import DATA_SIZES_LIST_BI_LEVEL

from src.challenges.six_field_test_data.six_generate_data import six_generate_data_file


def bi_level_generate_data_files(
        make_new_files: bool,
) -> None:
    for size in DATA_SIZES_LIST_BI_LEVEL:
        six_generate_data_file(
            data_description=size,
            make_new_files=make_new_files,
        )
