import time

import pandas as pd
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.six_field_test_data.six_runner_base import process_answer
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    Challenge, SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import RunResultBase

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldChallengeMethodPythonPysparkRegistration, SixFieldDataSetPyspark, pick_agg_tgt_num_partitions_pyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def test_one_step_in_pyspark_itinerary(
        challenge: Challenge,
        spark_session: TidySparkSession,
        exec_params: SixTestExecutionParameters,
        challenge_method_registration: SixFieldChallengeMethodPythonPysparkRegistration,
        result_columns: list[str],
        data_set: SixFieldDataSetPyspark,
        correct_answer: pd.DataFrame,
) -> RunResultBase | None:

    def check_partitions(rdd: RDD):
        logger = spark_session.logger
        agg_tgt_num_partitions = pick_agg_tgt_num_partitions_pyspark(data_set.data, challenge)
        if rdd.getNumPartitions() > max(agg_tgt_num_partitions, exec_params.default_parallelism):
            logger.info(f"{challenge_method_registration.strategy_name} output rdd "
                        f"has {rdd.getNumPartitions()} partitions")
            findings = rdd.collect()
            logger.info(f"size={len(findings)}, ", findings)
            exit(1)

    started_time = time.time()
    rdd_some: RDD
    match challenge_method_registration.delegate(
            spark_session=spark_session,
            exec_params=exec_params,
            data_set=data_set,
    ):
        case PySparkDataFrame() as spark_df:
            rdd_some = spark_df.rdd
            check_partitions(rdd_some)
            df_answer = spark_df.toPandas()
            finished_time = time.time()
        case RDD() as rdd_some:
            check_partitions(rdd_some)
            answer = rdd_some.collect()
            finished_time = time.time()
            if len(answer) == 0:
                df_answer = pd.DataFrame(data=[], columns=result_columns)
            else:
                match answer[0]:
                    case Row():
                        df_answer = pd.DataFrame.from_records([x.asDict() for x in answer])
                    case _:
                        df_answer = pd.DataFrame.from_records([x._asdict() for x in answer])
        case "infeasible":
            return None
        case _:
            raise ValueError("Must return at least 1 type")
    result = process_answer(
        challenge=challenge,
        data_description=data_set.data_description,
        correct_answer=correct_answer,
        numerical_tolerance=challenge_method_registration.numerical_tolerance,
        started_time=started_time,
        df_answer=df_answer,
        finished_time=finished_time,
    )
    return result
