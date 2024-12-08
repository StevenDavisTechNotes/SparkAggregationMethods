import time
from typing import Literal

import pandas as pd
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.six_field_test_data.six_runner_base import (
    process_answer,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    Challenge, SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import RunResultBase

from src.challenges.six_field_test_data.six_test_data_for_pyspark import (
    SixFieldChallengeMethodPythonPysparkRegistration, SixFieldDataSetPyspark,
    pick_agg_tgt_num_partitions_pyspark,
)
from src.utils.tidy_session_pyspark import TidySparkSession


def _call_delegate_with_timeout(
    *,
    challenge_method_registration: SixFieldChallengeMethodPythonPysparkRegistration,
    spark_session: TidySparkSession,
    exec_params: SixTestExecutionParameters,
    data_set: SixFieldDataSetPyspark,
):
    return challenge_method_registration.delegate(
        spark_session=spark_session,
        exec_params=exec_params,
        data_set=data_set,
    )


def six_run_one_step_in_pyspark_itinerary(
        *,
        challenge_method_registration: SixFieldChallengeMethodPythonPysparkRegistration,
        challenge: Challenge,
        correct_answer: pd.DataFrame,
        data_set: SixFieldDataSetPyspark,
        exec_params: SixTestExecutionParameters,
        result_columns: list[str],
        spark_session: TidySparkSession,
) -> (
    RunResultBase
    | tuple[Literal["infeasible"], str]
):

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
    match _call_delegate_with_timeout(
        challenge_method_registration=challenge_method_registration,
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
        case ("infeasible", reason):
            return ("infeasible", reason)
        case other:
            raise ValueError(
                "{strategy_name} unexpected returned a {other_type}"
                .format(
                    strategy_name=challenge_method_registration.strategy_name,
                    other_type=type(other)
                )
            )
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


def six_spark_config_base(
        exec_params: SixTestExecutionParameters,
) -> dict[str, str | int]:
    return {
        "spark.sql.shuffle.partitions": exec_params.default_parallelism,
        "spark.default.parallelism": exec_params.default_parallelism,
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
        "spark.executor.heartbeatInterval": "3600s",
        "spark.network.timeout": "36000s",
        "spark.shuffle.io.maxRetries": "10",
        "spark.shuffle.io.retryWait": "600s",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
    }
