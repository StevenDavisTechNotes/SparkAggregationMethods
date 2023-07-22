from typing import Iterable, Optional, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from Utils.SparkUtils import TidySparkSession
from SixFieldCommon.SixFieldTestData import MAX_DATA_POINTS_PER_PARTITION, DataPoint, DataSet, ExecutionParameters


def vanilla_rdd_grpmap(
    _spark_session: TidySparkSession,
    _exec_params: ExecutionParameters,
    data_set: DataSet
) -> Tuple[Optional[RDD], Optional[spark_DataFrame]]:

    def processData1(key: Tuple[int, int], iterator: Iterable[DataPoint]) -> Tuple[Tuple[int, int], Row]:
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
        return (
            key,
            Row(grp=key[0],
                subgrp=key[1],
                mean_of_C=mean_of_C,
                max_of_D=max_of_D,
                var_of_E=var_of_E))

    if (
            data_set.NumDataPoints
            > MAX_DATA_POINTS_PER_PARTITION
            * data_set.NumGroups * data_set.NumSubGroups
    ):
        raise ValueError(
            "This strategy only works if all of the values per key can fit into memory at once.")

    rddResult = (
        data_set.rddSrc
        .groupBy(lambda x: (x.grp, x.subgrp))
        .map(lambda pair: processData1(pair[0], pair[1]))
        .repartition(numPartitions=1)
        .sortByKey()  # type: ignore
        .values()
    )
    return rddResult, None
