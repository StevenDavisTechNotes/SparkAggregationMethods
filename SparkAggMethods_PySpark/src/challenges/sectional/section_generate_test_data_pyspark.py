import os

from spark_agg_methods_common_python.challenges.sectional.section_test_data_types import (
    DATA_SIZE_LIST_SECTIONAL, LARGEST_EXPONENT_SECTIONAL, SECTION_SIZE_MAXIMUM, derive_source_test_data_file_path,
)
from spark_agg_methods_common_python.utils.utils import int_divide_round_up

from src.challenges.sectional.section_test_data_types_pyspark import (
    ExecutionParameters, SectionDataSetPyspark, SectionPySparkExecutionParameters,
)


def populate_data_sets_pyspark(
        exec_params: ExecutionParameters,
) -> list[SectionDataSetPyspark]:
    datasets: list[SectionDataSetPyspark] = []
    num_students = 1
    for i_scale in range(0, LARGEST_EXPONENT_SECTIONAL + 1):
        num_students = 10**i_scale
        file_path = derive_source_test_data_file_path(num_students)
        data_size = num_students * SECTION_SIZE_MAXIMUM
        assert os.path.exists(file_path)
        src_num_partitions = max(
            exec_params.default_parallelism,
            int_divide_round_up(
                data_size,
                exec_params.maximum_processable_segment,
            ),
        )
        datasets.append(
            SectionDataSetPyspark(
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
