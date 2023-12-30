from dataclasses import dataclass
from typing import Dict, Iterable, NamedTuple, Tuple, cast

from pyspark import RDD
from pyspark.sql import Row

from six_field_test_data.six_generate_test_data_using_pyspark import (
    PysparkDataSet, PysparkPythonPendingAnswerSet)
from six_field_test_data.six_test_data_types import (DataPoint,
                                                     ExecutionParameters)
from utils.tidy_spark_session import TidySparkSession


@dataclass(frozen=False)
class MutableSubGrpTotal:
    grp: int
    subgrp: int
    running_count: int = 0
    running_sum_of_E_squared: float = 0
    running_sum_of_E: float = 0

    def __init__(self, grp: int, subgrp: int):
        self.grp = grp
        self.subgrp = subgrp
        self.running_count = 0
        self.running_sum_of_E_squared = 0
        self.running_sum_of_E = 0


class MutableGrpTotal:
    grp: int
    running_sum_of_C: float
    running_max_of_D: float | None
    running_subgrp_totals: dict[int, MutableSubGrpTotal]

    def __init__(self, grp: int):
        self.grp = grp
        self.running_sum_of_C = 0
        self.running_max_of_D = None
        self.running_subgrp_totals = {}


class SubTotal2(NamedTuple):
    grp: int
    subgrp: int
    running_sum_of_E_squared: float
    running_sum_of_E: float
    running_count: int


class SubTotal1(NamedTuple):
    grp: int
    running_sum_of_C: float
    running_max_of_D: float
    subgrp_totals: dict[int, SubTotal2]


def bi_level_pyspark_rdd_map_part(
        spark_session: TidySparkSession,
        _exec_params: ExecutionParameters,
        data_set: PysparkDataSet
) -> PysparkPythonPendingAnswerSet:
    rddSrc = data_set.data.rddSrc

    rddResult = cast(
        RDD[Row],
        rddSrc
        .mapPartitions(partitionTriage)
        .groupByKey(numPartitions=data_set.data.AggTgtNumPartitions)
        .map(lambda kv: (kv[0], mergeCombiners3(kv[0], kv[1])))
        .sortByKey()  # type: ignore
        .values()
    )
    return PysparkPythonPendingAnswerSet(rdd_row=rddResult)


def partitionTriage(
        iterator: Iterable[DataPoint]
) -> Iterable[Tuple[int, SubTotal1]]:
    running_grp_totals: Dict[int, MutableGrpTotal] = dict()
    for v in iterator:
        k1 = v.grp
        if k1 not in running_grp_totals:
            running_grp_totals[k1] = MutableGrpTotal(v.grp)
        r1 = running_grp_totals[k1]
        r1.running_sum_of_C += v.C
        r1.running_max_of_D = \
            r1.running_max_of_D \
            if r1.running_max_of_D is not None and \
            r1.running_max_of_D > v.D \
            else v.D
        k2 = v.subgrp
        if k2 not in r1.running_subgrp_totals:
            r1.running_subgrp_totals[k2] = MutableSubGrpTotal(v.grp, v.subgrp)
        r2 = r1.running_subgrp_totals[k2]
        r2.running_sum_of_E_squared += v.E * v.E
        r2.running_sum_of_E += v.E
        r2.running_count += 1
    for k1 in running_grp_totals:
        r1 = running_grp_totals[k1]
        assert r1.running_max_of_D is not None
        yield (
            k1,
            SubTotal1(
                grp=r1.grp,
                running_sum_of_C=r1.running_sum_of_C,
                running_max_of_D=r1.running_max_of_D,
                subgrp_totals={
                    k2:
                    SubTotal2(
                        grp=r2.grp,
                        subgrp=r2.subgrp,
                        running_sum_of_E_squared=r2.running_sum_of_E_squared,
                        running_sum_of_E=r2.running_sum_of_E,
                        running_count=r2.running_count)
                    for k2, r2 in r1.running_subgrp_totals.items()
                }
            )
        )


def mergeCombiners3(
        grp: int,
        iterable: Iterable[SubTotal1]
) -> Row:
    import statistics
    lsub = MutableGrpTotal(grp)
    for rsub1 in iterable:
        lsub.running_sum_of_C += rsub1.running_sum_of_C
        lsub.running_max_of_D = lsub.running_max_of_D \
            if lsub.running_max_of_D is not None and \
            lsub.running_max_of_D > rsub1.running_max_of_D \
            else rsub1.running_max_of_D
        for subgrp, rsub2 in rsub1.subgrp_totals.items():
            k2 = subgrp
            if k2 not in lsub.running_subgrp_totals:
                lsub.running_subgrp_totals[k2] = MutableSubGrpTotal(grp, subgrp)
            lsub2 = lsub.running_subgrp_totals[k2]
            lsub2.running_sum_of_E_squared += \
                rsub2.running_sum_of_E_squared
            lsub2.running_sum_of_E += rsub2.running_sum_of_E
            lsub2.running_count += rsub2.running_count
    running_count = 0
    vars_of_E = []
    for subgrp, lsub2 in lsub.running_subgrp_totals.items():
        var_of_E = (
            lsub2.running_sum_of_E_squared / lsub2.running_count
            - (lsub2.running_sum_of_E / lsub2.running_count)**2)
        vars_of_E.append(var_of_E)
        running_count += lsub2.running_count
    return Row(
        grp=grp,
        mean_of_C=lsub.running_sum_of_C / running_count,
        max_of_D=lsub.running_max_of_D,
        avg_var_of_E=statistics.mean(vars_of_E))
