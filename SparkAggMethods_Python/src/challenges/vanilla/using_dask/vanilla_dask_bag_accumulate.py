from dataclasses import asdict

import pandas as pd
from dask.bag.core import Bag as DaskBag

from src.challenges.vanilla.vanilla_test_data_types import RESULT_COLUMNS
from src.six_field_test_data.six_domain_logic import (accumulate_subtotal,
                                                      total_from_subtotal)
from src.six_field_test_data.six_generate_test_data import (
    DataSetDask, TChallengeAnswerPythonDask)
from src.six_field_test_data.six_test_data_types import (DataPointNT,
                                                         ExecutionParameters,
                                                         SubTotalDC)


def vanilla_dask_bag_accumulate(
        exec_params: ExecutionParameters,
        data_set: DataSetDask
) -> TChallengeAnswerPythonDask:
    if (data_set.data_size.points_per_index > 10**6):  # just takes too long
        return "infeasible"
    stage0: DaskBag = data_set.data.bag_src
    stage1 = (
        stage0
        .accumulate(in_key_in_partition_reduction, initial=dict())
        .compute()
    )
    stage2 = finalize(stage1)
    return stage2


def in_key_in_partition_reduction(
        acc: dict[tuple[int, int], SubTotalDC],
        element: DataPointNT,
) -> dict[tuple[int, int], SubTotalDC]:
    key = (element.grp, element.subgrp)
    prior = acc[key] if key in acc else None
    acc[key] = accumulate_subtotal(prior, element)
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
            } | asdict(total_from_subtotal(subtotal))
            for (grp, subgrp), subtotal in acc.items()
        ]
    )
    df = (
        df
        .loc[:, RESULT_COLUMNS]
        .sort_values(["grp", "subgrp"])
        .reset_index(drop=True)
    )
    return df
