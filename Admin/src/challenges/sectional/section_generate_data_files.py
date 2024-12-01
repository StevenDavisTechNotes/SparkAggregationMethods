import datetime as dt
import logging
import os
import random
from pathlib import Path

from spark_agg_methods_common_python.challenges.sectional.section_nospark_logic import section_nospark_logic
from spark_agg_methods_common_python.challenges.sectional.section_persist_test_data import AnswerFileSectional
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    DATA_SIZE_LIST_SECTIONAL, NUM_CLASSES_PER_TRIMESTER, NUM_DEPARTMENTS, NUM_TRIMESTERS, SectionDataSetDescription,
    StudentSummary, add_months_to_date_retracting, section_derive_expected_answer_data_file_path,
    section_derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.utils.pandas_helpers import make_pd_dataframe_from_list_of_named_tuples
from spark_agg_methods_common_python.utils.platform import setup_logging

logger = logging.getLogger(__name__)


def _remove_data_files_for_size(
        data_description: SectionDataSetDescription,
) -> None:
    source_file_path = section_derive_source_test_data_file_path(
        data_description=data_description,
    )
    answer_file_path = section_derive_expected_answer_data_file_path(
        data_description=data_description,
    )
    if os.path.exists(source_file_path):
        os.unlink(source_file_path)
    if os.path.exists(answer_file_path):
        os.unlink(answer_file_path)


def _generate_source_data_file_for_size(
        data_description: SectionDataSetDescription,
) -> None:
    final_file_name = section_derive_source_test_data_file_path(
        data_description, temp_file=False)
    temp_file_name = section_derive_source_test_data_file_path(
        data_description, temp_file=True)
    if os.path.exists(final_file_name):
        return
    if os.path.exists(temp_file_name):
        os.unlink(temp_file_name)
    num_students = data_description.num_students
    num_trimesters = NUM_TRIMESTERS
    num_departments = NUM_DEPARTMENTS
    num_classes_per_trimester = NUM_CLASSES_PER_TRIMESTER
    Path(temp_file_name).parent.mkdir(parents=True, exist_ok=True)
    with open(temp_file_name, "w") as f:
        logger.info(f"Creating {final_file_name}")
        for student_id in range(1, num_students + 1):
            f.write(f"S,{student_id},John{student_id}\n")
            for trimester in range(1, num_trimesters + 1):
                dated = add_months_to_date_retracting(dt.datetime(2017, 1, 1), trimester)
                was_abroad = random.randint(0, 10) == 0
                major = (student_id %
                         num_departments) if trimester > 1 else num_departments - 1
                f.write(f"TH,{dated:%Y-%m-%d},{was_abroad}\n")
                trimester_credits = 0
                trimester_weighted_grades = 0
                for _i_class in range(1, num_classes_per_trimester + 1):
                    dept = random.randrange(0, num_departments)
                    grade = random.randint(1, 4)
                    credits = random.randint(1, 5)
                    f.write(f"C,{dept},{grade},{credits}\n")
                    trimester_credits += credits
                    trimester_weighted_grades += grade * credits
                gpa = trimester_weighted_grades / trimester_credits
                f.write(f"TF,{major},{gpa},{trimester_credits}\n")
    Path(final_file_name).parent.mkdir(parents=True, exist_ok=True)
    os.rename(temp_file_name, final_file_name)


def _generate_answer_file(data_description: SectionDataSetDescription) -> None:
    answer_file_path = section_derive_expected_answer_data_file_path(data_description)
    if os.path.exists(answer_file_path):
        return
    answer_iterable = section_nospark_logic(
        data_description=data_description,
    )
    df = make_pd_dataframe_from_list_of_named_tuples(
        list(answer_iterable),
        row_type=StudentSummary
    )
    AnswerFileSectional.write_answer_file_sectional(data_description, df)


def sectional_generate_data_files(
        *,
        make_new_files: bool,
) -> None:
    data_descriptions = DATA_SIZE_LIST_SECTIONAL
    if make_new_files:
        for data_description in data_descriptions:
            _remove_data_files_for_size(data_description)
    for data_description in data_descriptions:
        _generate_source_data_file_for_size(data_description)
        _generate_answer_file(data_description)


def main():
    logger.info(f"Running {__file__}")
    try:
        sectional_generate_data_files(make_new_files=False)
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
        return
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    main()
