from typing import Iterable, cast

from pyspark import RDD
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    Challenge, DataPointNT, SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    MAX_DATA_POINTS_PER_SPARK_PARTITION, SixFieldDataSetPyspark, TSixFieldChallengePendingAnswerPythonPyspark,
    pick_agg_tgt_num_partitions_pyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession

CHALLENGE = Challenge.CONDITIONAL


def cond_pyspark_rdd_grp_map(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark,
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if (
            data_set.data_description.points_per_index
            > MAX_DATA_POINTS_PER_SPARK_PARTITION
    ):
        # This strategy only works if all of the values per key can fit into memory at once.
        return "infeasible", "Requires all values per key to fit in memory"
    agg_tgt_num_partitions = pick_agg_tgt_num_partitions_pyspark(data_set.data, CHALLENGE)
    rdd_src = data_set.data.open_source_data_as_rdd(spark_session)
    rdd_result = cast(
        RDD[Row],
        rdd_src
        .map(lambda r: DataPointNT(*r))
        .groupBy(lambda x: (x.grp, x.subgrp))
        .map(lambda pair: (pair[0], process_data_1(pair[0], pair[1])), preservesPartitioning=True)
        .sortByKey(numPartitions=agg_tgt_num_partitions)  # type: ignore
        .values()
    )
    return rdd_result


def process_data_1(
        key: tuple[int, int],
        iterator: Iterable[DataPointNT],
) -> Row:
    import math
    sum_of_C = 0
    unconditional_count = 0
    max_of_D: float = math.nan
    cond_sum_of_E_squared = 0
    cond_sum_of_E = 0
    cond_count_of_E = 0
    for item in iterator:
        sum_of_C = sum_of_C + item.C
        unconditional_count = unconditional_count + 1
        max_of_D = (item.D
                    if not math.isnan(max_of_D) or max_of_D < item.D
                    else max_of_D)
        if item.E < 0:
            cond_sum_of_E_squared = \
                cond_sum_of_E_squared + item.E * item.E
            cond_sum_of_E = cond_sum_of_E + item.E
            cond_count_of_E = cond_count_of_E + 1
    mean_of_C = sum_of_C / unconditional_count \
        if unconditional_count > 0 else math.nan
    cond_var_of_E = (
        cond_sum_of_E_squared / cond_count_of_E
        - (cond_sum_of_E / cond_count_of_E)**2
        if cond_count_of_E > 0 else math.nan)
    return Row(
        grp=key[0],
        subgrp=key[1],
        mean_of_C=mean_of_C,
        max_of_D=max_of_D,
        cond_var_of_E=cond_var_of_E)
