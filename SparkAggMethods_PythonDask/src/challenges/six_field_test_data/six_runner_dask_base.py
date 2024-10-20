import time

import pandas as pd
from dask.dataframe.core import DataFrame as DaskDataFrame
from spark_agg_methods_common_python.challenges.six_field_test_data.six_runner_base import process_answer
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    Challenge, SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import RunResultBase

from src.challenges.six_field_test_data.six_test_data_for_dask import (
    ChallengeMethodPythonDaskRegistration, DataSetDaskWithAnswer, pick_agg_tgt_num_partitions_dask,
)


def test_one_step_in_dask_itinerary(
        challenge: Challenge,
        exec_params: SixTestExecutionParameters,
        challenge_method_registration: ChallengeMethodPythonDaskRegistration,
        data_set: DataSetDaskWithAnswer,
) -> RunResultBase | None:
    started_time = time.time()
    agg_tgt_num_partitions = pick_agg_tgt_num_partitions_dask(data_set.data, challenge)
    df_answer: pd.DataFrame
    match challenge_method_registration.delegate(
        exec_params=exec_params,
        data_set=data_set,
    ):
        case DaskDataFrame() as ddf:
            if ddf.npartitions > max(agg_tgt_num_partitions, exec_params.default_parallelism):
                print(
                    f"{challenge_method_registration.strategy_name} output rdd has {ddf.npartitions} partitions")
                findings = ddf.compute()
                print(f"size={len(findings)}, ", findings)
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
        data_size=data_set.data_description,
        correct_answer=data_set.answer.answer_for_challenge(challenge),
        numerical_tolerance=challenge_method_registration.numerical_tolerance,
        started_time=started_time,
        df_answer=df_answer,
        finished_time=finished_time,
    )
    return result
