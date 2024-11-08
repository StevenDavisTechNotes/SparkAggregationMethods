import logging
import time

import pandas as pd
from dask.dataframe.core import DataFrame as DaskDataFrame
from spark_agg_methods_common_python.challenges.six_field_test_data.six_runner_base import process_answer
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    Challenge, SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import RunResultBase

from src.challenges.six_field_test_data.six_test_data_for_dask import (
    ChallengeMethodPythonDaskRegistration, SixTestDataSetDask, pick_agg_tgt_num_partitions_dask,
)

logger = logging.getLogger(__name__)


def run_one_step_in_dask_itinerary(
        challenge: Challenge,
        exec_params: SixTestExecutionParameters,
        challenge_method_registration: ChallengeMethodPythonDaskRegistration,
        data_set: SixTestDataSetDask,
        correct_answer: pd.DataFrame,
) -> RunResultBase | None:
    started_time = time.time()
    try:
        agg_tgt_num_partitions = pick_agg_tgt_num_partitions_dask(data_set.data, challenge)
        df_answer: pd.DataFrame
        match challenge_method_registration.delegate(
            exec_params=exec_params,
            data_set=data_set,
        ):
            case DaskDataFrame() as ddf:
                if ddf.npartitions > max(agg_tgt_num_partitions, exec_params.default_parallelism):
                    logger.info(f"{challenge_method_registration.strategy_name} "
                                f"output rdd has {ddf.npartitions} partitions")
                    findings = ddf.compute()
                    logger.info(f"size={len(findings)}, ", findings)
                    exit(1)
                df_answer = ddf.compute()
                finished_time = time.time()
            case pd.DataFrame() as df_answer:
                finished_time = time.time()
            case "infeasible":
                return None
            case _:
                raise ValueError("No result returned")
        result = process_answer(
            challenge=challenge,
            data_description=data_set.data_description,
            correct_answer=correct_answer,
            numerical_tolerance=challenge_method_registration.numerical_tolerance,
            started_time=started_time,
            df_answer=df_answer,
            finished_time=finished_time,
        )
    except Exception as e:
        logger.error(
            f"Error in {challenge_method_registration.strategy_name} "
            f"of {data_set.data_description.size_code}"
        )
        raise e
    return result
