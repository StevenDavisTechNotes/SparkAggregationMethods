import logging
import time

import pandas as pd
from spark_agg_methods_common_python.challenges.six_field_test_data.six_runner_base import process_answer
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    Challenge, SixTestExecutionParameters,
)
from spark_agg_methods_common_python.perf_test_common import NumericalToleranceExpectations, RunResultBase

from src.challenges.six_field_test_data.six_test_data_for_py_only import (
    ChallengeMethodPythonOnlyRegistration, SixDataSetPythonOnly,
)

logger = logging.getLogger(__name__)


def _call_delegate_with_timeout(
    *,
    challenge_method_registration: ChallengeMethodPythonOnlyRegistration,
    exec_params: SixTestExecutionParameters,
        data_set: SixDataSetPythonOnly,
):
    return challenge_method_registration.delegate(
        exec_params=exec_params,
        data_set=data_set,
    )


def run_one_step_in_python_only_itinerary(
        challenge: Challenge,
        exec_params: SixTestExecutionParameters,
        challenge_method_registration: ChallengeMethodPythonOnlyRegistration,
        numerical_tolerance: NumericalToleranceExpectations,
        data_set: SixDataSetPythonOnly,
        correct_answer: pd.DataFrame,
) -> RunResultBase | None:
    started_time = time.time()
    try:
        match _call_delegate_with_timeout(
            challenge_method_registration=challenge_method_registration,
            exec_params=exec_params,
            data_set=data_set,
        ):
            case pd.DataFrame() as pandas_df:
                df_answer = pandas_df
                finished_time = time.time()
            case "infeasible":
                return None
            case None:
                return None
            case _:
                raise ValueError("Must return at least 1 type")
        result = process_answer(
            challenge=challenge,
            data_description=data_set.data_description,
            correct_answer=correct_answer,
            numerical_tolerance=numerical_tolerance,
            started_time=started_time,
            df_answer=df_answer,
            finished_time=finished_time,
        )
    except KeyboardInterrupt:
        return None
    except Exception as ex:
        msg = (
            f"Error in {challenge_method_registration.strategy_name} "
            f"of {data_set.data_description.size_code}"
            + ":"+str(ex)
        )
        logger.error(msg)
        raise Exception(msg)
    return result
