import datetime as dt
import os
import random
from typing import List

from challenges.sectional.section_test_data_types import (DataSet, DataSetData,
                                                          DataSetDescription,
                                                          ExecutionParameters,
                                                          NumDepts)
from utils.utils import int_divide_round_up

TEST_DATA_FILE_LOCATION = 'd:/temp/SparkPerfTesting'
NUM_TRIMESTERS = 8
NUM_CLASSES_PER_TRIMESTER = 4
SECTION_SIZE_MAXIMUM = (1 + NUM_TRIMESTERS * (1 + NUM_CLASSES_PER_TRIMESTER + 1))


LARGEST_EXPONENT = 7  # some can operate at 8 or above
AVAILABLE_DATA_SIZES: List[str] = [
    str(10**i) for i in range(0, LARGEST_EXPONENT + 1)]


def AddMonths(
        d: dt.date,
        add_months: int,
) -> dt.date:
    serial = d.year * 12 + (d.month - 1)
    serial += add_months
    return dt.date(serial // 12, serial % 12 + 1, d.day)


def populate_data_files(
        filename: str,
        NumStudents: int,
        NumTrimesters: int,
        NumClassesPerTrimester: int,
        NumDepts: int
) -> None:
    tmp_file_name = os.path.join(
        TEST_DATA_FILE_LOCATION,
        "Section_Test_Data",
        "section_testdata_temp.csv")
    with open(tmp_file_name, "w") as f:
        print(f"Creating {filename}")
        for studentId in range(1, NumStudents + 1):
            f.write(f"S,{studentId},John{studentId}\n")
            for trimester in range(1, NumTrimesters + 1):
                dated = AddMonths(dt.datetime(2017, 1, 1), trimester)
                wasAbroad = random.randint(0, 10) == 0
                major = (studentId %
                         NumDepts) if trimester > 1 else NumDepts - 1
                f.write(f"TH,{dated:%Y-%m-%d},{wasAbroad}\n")
                trimester_credits = 0
                trimester_weighted_grades = 0
                for _classno in range(1, NumClassesPerTrimester + 1):
                    dept = random.randrange(0, NumDepts)
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
) -> List[DataSet]:
    datasets: List[DataSet] = []
    num_students = 1
    for i_scale in range(0, LARGEST_EXPONENT + 1):
        num_students = 10**i_scale
        filename = os.path.join(
            TEST_DATA_FILE_LOCATION,
            "Section_Test_Data",
            f"section_testdata{num_students}.csv")
        data_size = num_students * SECTION_SIZE_MAXIMUM
        if make_new_files is True or os.path.exists(filename) is False:
            populate_data_files(filename, num_students, NUM_TRIMESTERS,
                                NUM_CLASSES_PER_TRIMESTER, NumDepts)
        src_num_partitions = max(
            exec_params.DefaultParallelism,
            int_divide_round_up(
                data_size,
                exec_params.MaximumProcessableSegment))
        datasets.append(
            DataSet(
                description=DataSetDescription(
                    size_code=str(num_students),
                    num_rows=data_size,
                    num_students=num_students,
                ),
                data=DataSetData(
                    section_maximum=SECTION_SIZE_MAXIMUM,
                    test_filepath=filename,
                    target_num_partitions=src_num_partitions,
                ),
                exec_params=exec_params,
            ))
    return datasets
