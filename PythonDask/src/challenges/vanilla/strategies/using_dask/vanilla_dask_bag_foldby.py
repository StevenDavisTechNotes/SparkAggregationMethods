from dataclasses import asdict

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
from spark_agg_methods_common_python.utils.ensure_has_memory import check_memory

from src.challenges.six_field_test_data.six_test_data_for_dask import (
    SixTestDataSetDask, TChallengeAnswerPythonDask,
)


def vanilla_dask_bag_foldby(
        exec_params: SixTestExecutionParameters,
        data_set: SixTestDataSetDask
) -> TChallengeAnswerPythonDask:
    if (data_set.data_description.points_per_index >= 10**6):  # EOM before calling accumulator
        return "infeasible", "EOM before calling accumulator"
    check_memory(throw=True)
    stage0: DaskBag = data_set.data.open_source_data_as_bag()
    stage1 = dict(
        stage0
        .map(lambda x: DataPointNT(*x))
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
    return naive_accumulation.accumulate_subtotal(prior, element)


def combine_subtotals(
        lhs: SubTotalDC,
        rhs: SubTotalDC,
) -> SubTotalDC:
    return naive_accumulation.combine_subtotals(lhs, rhs)


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
