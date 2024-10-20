from typing import Iterable

from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    MAX_DATA_POINTS_PER_SPARK_PARTITION, Challenge, DataPointNT, SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark, pick_agg_tgt_num_partitions_pyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession

CHALLENGE = Challenge.VANILLA


def vanilla_pyspark_rdd_grp_map(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:

    if (
            data_set.data_description.num_source_rows
            > MAX_DATA_POINTS_PER_SPARK_PARTITION
            * data_set.data_description.num_grp_1 * data_set.data_description.num_grp_2
    ):
        # This strategy only works if all of the values per key can fit into memory at once.
        return "infeasible"
    agg_tgt_num_partitions = pick_agg_tgt_num_partitions_pyspark(data_set.data, CHALLENGE)

    rddResult = (
        data_set.data.rdd_src
        .groupBy(lambda x: (x.grp, x.subgrp))
        .map(lambda pair: (pair[0], process_data_1(pair[0], pair[1])), preservesPartitioning=True)
        .sortByKey(numPartitions=agg_tgt_num_partitions)  # type: ignore
        .values()
    )
    return rddResult


def process_data_1(
        key: tuple[int, int],
        iterator: Iterable[DataPointNT],
) -> Row:
    import math
    sum_of_C = 0
    count = 0
    max_of_D = None
    sum_of_E_squared = 0
    sum_of_E = 0
    for item in iterator:
        sum_of_C = sum_of_C + item.C
        count = count + 1
        max_of_D = item.D \
            if max_of_D is None or max_of_D < item.D \
            else max_of_D
        sum_of_E_squared += item.E * item.E
        sum_of_E += item.E
    mean_of_C = sum_of_C / count \
        if count > 0 else math.nan
    var_of_E = sum_of_E_squared / count - (sum_of_E / count)**2
    return Row(grp=key[0],
               subgrp=key[1],
               mean_of_C=mean_of_C,
               max_of_D=max_of_D,
               var_of_E=var_of_E)
