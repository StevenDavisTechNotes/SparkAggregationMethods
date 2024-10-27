from spark_agg_methods_common_python.challenges.sectional.section_nospark_logic import section_nospark_logic
from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import StudentSummary

from src.challenges.sectional.section_test_data_types_pyspark import SectionDataSetPyspark


def section_py_only_single_threaded(
    data_set: SectionDataSetPyspark,
) -> list[StudentSummary]:
    return list(section_nospark_logic(
        data_description=data_set.data_description,
    ))
