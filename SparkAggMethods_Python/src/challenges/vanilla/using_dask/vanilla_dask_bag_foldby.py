from dataclasses import asdict

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


def vanilla_dask_bag_foldby(
        exec_params: ExecutionParameters,
        data_set: DataSetDask
) -> TChallengeAnswerPythonDask:
    check_memory(throw=True)
    stage0: DaskBag = data_set.data.bag_src
    stage1 = dict(
        stage0
        .foldby(
            key=lambda x: (x.grp, x.subgrp),
            binop=combine_with_running_subtotal,
            combine=combine_subtotals,
            initial=None)
        .compute()
    )
    stage2 = finalize(stage1)
    return stage2


def combine_with_running_subtotal(
        prior: SubTotalDC | None,
        element: DataPointNT,
) -> SubTotalDC:
    return six_domain_logic.accumulate_subtotal(prior, element)


def combine_subtotals(
        lhs: SubTotalDC,
        rhs: SubTotalDC,
) -> SubTotalDC:
    return six_domain_logic.combine_subtotals(lhs, rhs)


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
