import logging
from dataclasses import asdict

import pandas as pd
from dask.bag.core import Bag as DaskBag
from spark_agg_methods_common_python.challenges.six_field_test_data.six_domain_logic import naive_accumulation
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    DataPointNT, SixTestExecutionParameters, SubTotalDC,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import VANILLA_RESULT_COLUMNS

from src.challenges.six_field_test_data.six_test_data_for_dask import SixTestDataSetDask, TChallengeAnswerPythonDask

logger = logging.getLogger(__name__)


def vanilla_dask_bag_accumulate(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask
) -> TChallengeAnswerPythonDask:
    if (data_set.data_description.points_per_index >= 10**6):  # EOM before calling accumulator
        return "infeasible", "EOM before calling accumulator"
    stage0: DaskBag = data_set.data.open_source_data_as_bag()
    stage1 = (
        stage0
        .map(lambda x: DataPointNT(*x))
        .accumulate(combine_with_running_subtotal, initial=dict())
        .compute()
    )
    stage2 = finalize(stage1)
    return stage2


def combine_with_running_subtotal(
        acc: dict[tuple[int, int], SubTotalDC],
        element: DataPointNT,
) -> dict[tuple[int, int], SubTotalDC]:
    key = (element.grp, element.subgrp)
    logger.info(f"Processing {key}")
    prior = acc[key] if key in acc else None
    acc[key] = naive_accumulation.accumulate_subtotal(prior, element)
    return acc


def finalize(
        subtotal_list: list[dict[tuple[int, int], SubTotalDC]],
) -> pd.DataFrame:
    acc = dict()
    for subtotal in subtotal_list:
        for key, value in subtotal.items():
            acc[key] = value
    df = pd.DataFrame.from_records(
        [
            {
                "grp": grp,
                "subgrp": subgrp,
            }
            | asdict(naive_accumulation.total_from_subtotal(subtotal))
            for (grp, subgrp), subtotal in acc.items()
        ]
    )
    df = (
        df
        .loc[:, VANILLA_RESULT_COLUMNS]
        .sort_values(["grp", "subgrp"])
        .reset_index(drop=True)
    )
    return df
