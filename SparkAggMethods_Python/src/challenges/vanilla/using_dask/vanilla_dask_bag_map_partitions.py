from dataclasses import asdict
from typing import Iterable

import pandas as pd
from dask.bag.core import Bag as DaskBag

from src.challenges.vanilla.vanilla_test_data_types import RESULT_COLUMNS
from src.six_field_test_data import six_domain_logic
from src.six_field_test_data.six_generate_test_data import (
    DataSetDask, TChallengeAnswerPythonDask)
from src.six_field_test_data.six_test_data_types import (DataPointNT,
                                                         ExecutionParameters,
                                                         SubTotalDC)
from src.utils.ensure_has_memory import check_memory


def vanilla_dask_bag_map_partitions(
        exec_params: ExecutionParameters,
        data_set: DataSetDask
) -> TChallengeAnswerPythonDask:
    check_memory(throw=True)
    stage0: DaskBag = data_set.data.bag_src
    stage1 = (
        stage0
        .map_partitions(
            combine_within_partition,
        )
        .compute()
    )
    stage2 = combine_subtotals(stage1)
    stage3 = finalize(stage2)
    return stage3


def combine_within_partition(
        lst: Iterable[DataPointNT],
) -> Iterable[tuple[tuple[int, int], SubTotalDC]]:
    acc = dict()
    for element in lst:
        key = (element.grp, element.subgrp)
        prior = acc[key] if key in acc else None
        acc[key] = six_domain_logic.accumulate_subtotal(prior, element)
    return acc.items()


def combine_subtotals(
        lst: Iterable[tuple[tuple[int, int], SubTotalDC]],
) -> dict[tuple[int, int], SubTotalDC]:
    acc = dict()
    for key, subtotal in lst:
        acc[key] = six_domain_logic.combine_subtotals(acc.get(key), subtotal)
    return acc


def finalize(
        acc: dict[tuple[int, int], SubTotalDC],
) -> pd.DataFrame:
    df = pd.DataFrame.from_records(
        [
            {
                "grp": grp,
                "subgrp": subgrp,
            }
            | asdict(six_domain_logic.total_from_subtotal(subtotal))
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
