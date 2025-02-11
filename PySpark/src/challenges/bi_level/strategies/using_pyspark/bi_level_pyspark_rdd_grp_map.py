from typing import Iterable, cast

from pyspark import RDD
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    Challenge, DataPointNT, SixTestExecutionParameters,
)

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    MAX_DATA_POINTS_PER_SPARK_PARTITION, SixFieldDataSetPyspark,
    TSixFieldChallengePendingAnswerPythonPyspark,
    pick_agg_tgt_num_partitions_pyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession

CHALLENGE = Challenge.BI_LEVEL


def bi_level_pyspark_rdd_grp_map(
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        data_set: SixFieldDataSetPyspark
) -> TSixFieldChallengePendingAnswerPythonPyspark:
    if (
            data_set.data_description.num_source_rows
            > MAX_DATA_POINTS_PER_SPARK_PARTITION
            * data_set.data_description.num_grp_1
    ):
        # This strategy only works if all of the values per key can fit into memory at once.
        return "infeasible", "requires all values per key to fit in memory"
    rdd_src: RDD[Row] = data_set.data.open_source_data_as_rdd(spark_session)
    agg_tgt_num_partitions = pick_agg_tgt_num_partitions_pyspark(data_set.data, CHALLENGE)

    rdd_result = cast(
        RDD[Row],
        rdd_src
        .map(lambda r: DataPointNT(*r))
        .groupBy(lambda x: x.grp)
        .map(lambda pair: (
            pair[0],
            process_data_1(pair[0], pair[1])),
            preservesPartitioning=True)
        .sortByKey(numPartitions=agg_tgt_num_partitions)  # type: ignore
        .values()
    )

    return rdd_result


class MutableRunningTotal:
    running_sub_sum_of_E_squared: float
    running_sub_sum_of_E: float
    running_sub_count: int

    def __init__(
            self,
            grp: int,
    ):
        self.grp = grp
        self.running_sub_sum_of_E_squared = 0
        self.running_sub_sum_of_E = 0
        self.running_sub_count = 0


def process_data_1(
        grp: int,
        iterator: Iterable[DataPointNT]
) -> Row:
    import math
    import statistics
    running_sum_of_C = 0
    running_grp_count = 0
    running_max_of_D = None
    running_subs_of_E: dict[int, MutableRunningTotal] = dict()

    for item in iterator:
        running_sum_of_C += item.C
        running_grp_count += 1
        running_max_of_D = item.D \
            if running_max_of_D is None or \
            running_max_of_D < item.D \
            else running_max_of_D
        if item.subgrp not in running_subs_of_E:
            running_subs_of_E[item.subgrp] = MutableRunningTotal(grp)
        running_sub = running_subs_of_E[item.subgrp]
        running_sub.running_sub_sum_of_E_squared += \
            item.E * item.E
        running_sub.running_sub_sum_of_E += item.E
        running_sub.running_sub_count += 1
    mean_of_C = running_sum_of_C / running_grp_count \
        if running_grp_count > 0 else math.nan
    ar = [(
        x.running_sub_sum_of_E_squared / x.running_sub_count
        - (x.running_sub_sum_of_E / x.running_sub_count)**2)
        for x in running_subs_of_E.values()]
    avg_var_of_E = statistics.mean(ar)
    return Row(grp=grp,
               mean_of_C=mean_of_C,
               max_of_D=running_max_of_D,
               avg_var_of_E=avg_var_of_E)
