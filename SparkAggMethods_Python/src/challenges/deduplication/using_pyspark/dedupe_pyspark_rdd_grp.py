
from challenges.deduplication.dedupe_test_data_types import (
    DataSet, ExecutionParameters, TPysparkPythonPendingAnswerSet)
from challenges.deduplication.domain_logic.dedupe_domain_methods import (
    blocking_function, single_pass_rec_list)
from utils.tidy_spark_session import TidySparkSession


def dedupe_pyspark_rdd_grp(
        spark_session: TidySparkSession,
        data_params: ExecutionParameters,
        data_set: DataSet,
) -> TPysparkPythonPendingAnswerSet:
    if data_set.data_size > 50200:
        return "infeasible"
    dfSrc = data_set.df

    rdd = (
        dfSrc.rdd
        .groupBy(blocking_function, data_set.grouped_num_partitions)
        .flatMapValues(lambda iter:
                       single_pass_rec_list(list(iter)))
        .values())
    return rdd
