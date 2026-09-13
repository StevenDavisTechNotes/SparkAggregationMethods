from dataclasses import asdict
from typing import Iterable

import pandas as pd
from dask.bag.core import Bag as DaskBag
from spark_agg_methods_common_python.challenges.six_field_test_data.six_domain_logic import (
    naive_accumulation,
)
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    DataPointNT, SixTestExecutionParameters, SubTotalDC,
)
from spark_agg_methods_common_python.challenges.vanilla.vanilla_test_data_types import (
    VANILLA_RESULT_COLUMNS,
)
from spark_agg_methods_common_python.utils.ensure_has_memory import (
    is_memory_low,
)

from src.challenges.six_field_test_data.six_test_data_for_dask import (
    SixTestDataSetDask, TChallengeAnswerPythonDask,
)


def vanilla_dask_bag_map_partitions(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask
) -> TChallengeAnswerPythonDask:
    if (data_set.data_description.points_per_index >= 10**6):  # EOM before calling accumulator
        return "infeasible", "EOM before calling accumulator"
    is_memory_low(throw=True)
    stage0: DaskBag = data_set.data.open_source_data_as_bag()
    stage1 = (
        stage0
        .map(lambda x: DataPointNT(*x))
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
        acc[key] = naive_accumulation.accumulate_subtotal(prior, element)
    return acc.items()


def combine_subtotals(
        lst: Iterable[tuple[tuple[int, int], SubTotalDC]],
) -> dict[tuple[int, int], SubTotalDC]:
    acc = dict()
    for key, subtotal in lst:
        acc[key] = naive_accumulation.combine_subtotals(acc.get(key), subtotal)
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
