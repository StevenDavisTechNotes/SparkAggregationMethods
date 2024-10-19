from dataclasses import asdict

import pandas as pd
from dask.bag.core import Bag as DaskBag
from spark_agg_methods_common_python.utils.ensure_has_memory import check_memory

from src.challenges.vanilla.vanilla_test_data_types import RESULT_COLUMNS
from src.six_field_test_data import six_domain_logic
from src.six_field_test_data.six_generate_test_data import DataSetDask, TChallengeAnswerPythonDask
from src.six_field_test_data.six_test_data_types import DataPointNT, SixTestExecutionParameters, SubTotalDC


def vanilla_dask_bag_fold(
        exec_params: SixTestExecutionParameters,
        data_set: DataSetDask
) -> TChallengeAnswerPythonDask:
    check_memory(throw=True)
    stage0: DaskBag = data_set.data.bag_src
    stage1 = (
        stage0
        .fold(
            binop=combine_with_running_subtotal,
            combine=combine_subtotals,
            initial=dict())
        .compute()
    )
    stage2 = finalize(stage1)
    return stage2


def combine_with_running_subtotal(
        acc: dict[tuple[int, int], SubTotalDC],
        element: DataPointNT,
) -> dict[tuple[int, int], SubTotalDC]:
    key = (element.grp, element.subgrp)
    prior = acc[key] if key in acc else None
    acc[key] = six_domain_logic.accumulate_subtotal(prior, element)
    return acc


def combine_subtotals(
        lhs: dict[tuple[int, int], SubTotalDC],
        rhs: dict[tuple[int, int], SubTotalDC],
) -> dict[tuple[int, int], SubTotalDC]:
    acc = dict()
    for key in set(lhs.keys()).union(rhs.keys()):
        acc[key] = six_domain_logic.combine_subtotals(lhs.get(key), rhs.get(key))
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
