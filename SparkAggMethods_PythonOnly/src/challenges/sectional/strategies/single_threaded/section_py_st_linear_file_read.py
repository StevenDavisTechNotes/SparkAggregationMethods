import pandas as pd
from spark_agg_methods_common_python.challenges.sectional.section_nospark_logic import section_nospark_logic
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import StudentSummary

from src.challenges.sectional.section_test_data_types_py_only import (
    SectionDataSetPyOnly, SectionExecutionParametersPyOnly,
)


def section_py_st_linear_file_read(
        data_set: SectionDataSetPyOnly,
        exec_params: SectionExecutionParametersPyOnly,
) -> list[StudentSummary] | pd.DataFrame:
    return list(section_nospark_logic(
        data_description=data_set.data_description,
    ))
