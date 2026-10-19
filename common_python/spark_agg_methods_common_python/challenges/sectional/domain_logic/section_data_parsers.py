import re
from pathlib import Path

from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    ClassLine, StudentHeader, TrimesterFooter, TrimesterHeader,
)
from spark_agg_methods_common_python.perf_test_common import LOCAL_TEST_DATA_FILE_LOCATION


def parse_line_to_types(
        line: str,
) -> StudentHeader | TrimesterHeader | ClassLine | TrimesterFooter:
    fields = line.rstrip().split(",")
    if fields[0] == "S":
        return StudentHeader(StudentId=int(fields[1]), StudentName=fields[2])
    if fields[0] == "TH":
        return TrimesterHeader(Date=fields[1], WasAbroad=(fields[2] == "True"))
    if fields[0] == "C":
        return ClassLine(Dept=int(fields[1]), Credits=int(
            fields[2]), Grade=int(fields[3]))
    if fields[0] == "TF":
        return TrimesterFooter(Major=int(fields[1]), GPA=float(
            fields[2]), Credits=int(fields[3]))
    raise Exception("Malformed data " + line)


def identify_section_using_intermediate_file(
        src_file_path: Path,
) -> Path:
    dest_file_path = LOCAL_TEST_DATA_FILE_LOCATION / "temp.csv"
    dest_file_path.unlink(missing_ok=True)
    extra_type_pattern = re.compile("^S,")
    section_id = -1
    with dest_file_path.open("w") as out_fh:
        with src_file_path.open("r") as in_fh:
            for line in in_fh:
                if extra_type_pattern.match(line):
                    section_id += 1
                if section_id < 0:
                    raise ValueError(
                        f"First line must start a section, found: {line}"
                    )
                out_fh.write(f"{section_id},{line}")
    return dest_file_path
