import datetime as dt
import os
import random
from pathlib import Path

from spark_agg_methods_common_python.utils.utils import int_divide_round_up

from src.challenges.sectional.section_pyspark_test_data_types import (
    ExecutionParameters, NumDepartments, SectionDataSet, SectionDataSetDescription, SectionPySparkExecutionParameters,
)

TEST_DATA_FILE_LOCATION = 'd:/temp/SparkPerfTesting'
NUM_TRIMESTERS = 8
NUM_CLASSES_PER_TRIMESTER = 4
SECTION_SIZE_MAXIMUM = (1 + NUM_TRIMESTERS * (1 + NUM_CLASSES_PER_TRIMESTER + 1))


LARGEST_EXPONENT = 7  # some can operate at 8 or above
DATA_SIZE_LIST_SECTIONAL = [
    SectionDataSetDescription(
        i_scale=i_scale,
        num_students=10**i_scale,
        section_size_max=SECTION_SIZE_MAXIMUM,
    )
    for i_scale in range(0, LARGEST_EXPONENT + 1)
]


def add_months(
        d: dt.date,
        add_months: int,
) -> dt.date:
    serial = d.year * 12 + (d.month - 1)
    serial += add_months
    return dt.date(serial // 12, serial % 12 + 1, d.day)


def populate_data_files(
        filename: str,
        num_students: int,
        num_trimesters: int,
        num_classes_per_trimester: int,
        num_departments: int
) -> None:
    tmp_file_name = os.path.join(
        TEST_DATA_FILE_LOCATION,
        "Section_Test_Data",
        "section_testdata_temp.csv")
    Path(tmp_file_name).parent.mkdir(parents=True, exist_ok=True)
    with open(tmp_file_name, "w") as f:
        print(f"Creating {filename}")
        for student_id in range(1, num_students + 1):
            f.write(f"S,{student_id},John{student_id}\n")
            for trimester in range(1, num_trimesters + 1):
                dated = add_months(dt.datetime(2017, 1, 1), trimester)
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
    os.rename(tmp_file_name, filename)


def populate_data_sets(
        exec_params: ExecutionParameters,
        make_new_files: bool,
) -> list[SectionDataSet]:
    datasets: list[SectionDataSet] = []
    num_students = 1
    for i_scale in range(0, LARGEST_EXPONENT + 1):
        num_students = 10**i_scale
        file_path = os.path.join(
            TEST_DATA_FILE_LOCATION,
            "Section_Test_Data",
            f"section_testdata{num_students}.csv")
        data_size = num_students * SECTION_SIZE_MAXIMUM
        if make_new_files is True or os.path.exists(file_path) is False:
            populate_data_files(file_path, num_students, NUM_TRIMESTERS,
                                NUM_CLASSES_PER_TRIMESTER, NumDepartments)
        src_num_partitions = max(
            exec_params.default_parallelism,
            int_divide_round_up(
                data_size,
                exec_params.maximum_processable_segment))
        datasets.append(
            SectionDataSet(
                data_description=DATA_SIZE_LIST_SECTIONAL[i_scale],
                exec_params=SectionPySparkExecutionParameters(
                    default_parallelism=exec_params.default_parallelism,
                    maximum_processable_segment=exec_params.maximum_processable_segment,
                    test_data_folder_location=exec_params.test_data_folder_location,
                    section_maximum=SECTION_SIZE_MAXIMUM,
                    source_data_file_path=file_path,
                    target_num_partitions=src_num_partitions,
                ),
            ))
    return datasets
