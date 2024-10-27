from spark_agg_methods_common_python.challenges.sectional.section_persist_test_data import \
    populate_data_sets_base


def generate_data_sectional(
        *,
        make_new_files: bool,
) -> None:
    populate_data_sets_base(make_new_files=make_new_files)


if __name__ == "__main__":
    print(f"Running {__file__}")
    generate_data_sectional(make_new_files=False)
    print("Done!")
