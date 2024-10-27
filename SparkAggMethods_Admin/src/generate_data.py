#!python
# usage: python -m src.generate_data

from src.challenges.sectional.generate_data_sectional import \
    generate_data_sectional

MAKE_NEW_FILES: bool = False


def main(
        *,
        make_new_files: bool,
) -> None:
    print(f"Running {__file__}")
    try:
        generate_data_sectional(make_new_files=make_new_files,)
    except KeyboardInterrupt:
        print("Interrupted!")
        return
    print("Done!")


if __name__ == "__main__":
    main(make_new_files=MAKE_NEW_FILES)
