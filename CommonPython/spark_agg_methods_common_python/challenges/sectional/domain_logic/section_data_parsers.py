import os
import re

from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    ClassLine, StudentHeader, TrimesterFooter, TrimesterHeader,
)
from spark_agg_methods_common_python.perf_test_common import (
    LOCAL_TEST_DATA_FILE_LOCATION,
)


def parse_line_to_types(
        line: str,
) -> StudentHeader | TrimesterHeader | ClassLine | TrimesterFooter:
    fields = line.rstrip().split(',')
    if fields[0] == 'S':
        return StudentHeader(StudentId=int(fields[1]), StudentName=fields[2])
    if fields[0] == 'TH':
        return TrimesterHeader(Date=fields[1], WasAbroad=(fields[2] == 'True'))
    if fields[0] == 'C':
        return ClassLine(Dept=int(fields[1]), Credits=int(
            fields[2]), Grade=int(fields[3]))
    if fields[0] == 'TF':
        return TrimesterFooter(Major=int(fields[1]), GPA=float(
            fields[2]), Credits=int(fields[3]))
    raise Exception("Malformed data " + line)


def identify_section_using_intermediate_file(
        src_file_path: str,
) -> str:
    dest_file_path = f"{LOCAL_TEST_DATA_FILE_LOCATION}/temp.csv"
    if os.path.exists(dest_file_path):
        os.unlink(dest_file_path)
    extra_type_pattern = re.compile("^S,")
    section_id = -1
    with open(dest_file_path, "w") as out_fh:
        with open(src_file_path, "r") as in_fh:
            for line in in_fh:
                if extra_type_pattern.match(line):
                    section_id += 1
                assert section_id >= 0
                out_fh.write(f"{section_id},{line}")
    return dest_file_path
