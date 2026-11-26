import pandas as pd
from spark_agg_methods_common_python.challenges.sectional.sectional_nospark_logic import sectional_nospark_logic
from spark_agg_methods_common_python.challenges.sectional.sectional_test_data_types import StudentSummary
from src.challenges.sectional.sectional_test_data_types_py_st import (
    SectionalDataSetPyST, SectionalExecutionParametersPyOnly,
)


def sectional_py_st_linear_file_read(
        data_set: SectionalDataSetPyST,
        exec_params: SectionalExecutionParametersPyOnly,
) -> list[StudentSummary] | pd.DataFrame:
    return list(sectional_nospark_logic(
        data_description=data_set.data_description,
    ))
