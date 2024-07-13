# cSpell: ignore Plaineville, FFFFFFA, LLLLLLA
from typing import Iterable

from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row

from src.challenges.deduplication.dedupe_generate_test_data import name_hash
from src.challenges.deduplication.dedupe_test_data_types import (
    ItineraryItem, RecordSparseStruct)


def arrange_field_order(
        rec: Row,
) -> Row:
    row = Row(*(
        rec.FirstName,
        rec.LastName,
        rec.StreetAddress,
        rec.City,
        rec.ZipCode,
        rec.SecretKey,
        rec.FieldA,
        rec.FieldB,
        rec.FieldC,
        rec.FieldD,
        rec.FieldE,
        rec.FieldF))
    row.__fields__ = RecordSparseStruct.names
    return row


def verify_correctness(
        itinerary_items: ItineraryItem,
        lst: list[Row],
):
    try:
        lst.sort(key=lambda x: int(x.FieldA))
        actual_num_people = itinerary_items.data_set.num_people
        num_sources = itinerary_items.data_set.num_sources
        secret_keys = {x.SecretKey for x in lst}
        expected_secret_keys = set(range(1, actual_num_people + 1))
        if secret_keys != expected_secret_keys:
            missing_keys = expected_secret_keys - secret_keys
            extra_keys = secret_keys - expected_secret_keys
            raise Exception(f"Missing {missing_keys} extra {extra_keys}")

        count = len(lst)
        if count != actual_num_people:
            raise Exception(
                f"df.count()({count}) != numPeople({actual_num_people}) ")
        NumBRecords = max(1, 2 * actual_num_people // 100)
        for index, row in enumerate(lst):
            verify_single_line(num_sources, NumBRecords, index, row)
    except Exception as exception:
        print("data error", exception)
        return False
    return True


def verify_single_line(  # noqa: C901
        NumSources: int,
        NumBRecords: int,
        index: int,
        row: Row) -> None:
    i = index + 1
    if f"FFFFFFA{i}_{name_hash(i)}" != row.FirstName:
        raise Exception(
            f"{i}: FFFFFFA{i}_{name_hash(i)} != {row.FirstName}")
    if f'LLLLLLA{i}_{name_hash(i)}' != row.LastName:
        raise Exception(
            f'{i}: LLLLLLA{i}_{name_hash(i)} != {row.LastName}')
    if f'{i} Main St' != row.StreetAddress:
        raise Exception(f'{i} Main St != row.StreetAddress')
    if 'Plaineville ME' != row.City:
        raise Exception('{i}: Plaineville ME != {row.City}')
    if f'{(i-1) % 100:05d}' != row.ZipCode:
        raise Exception(f'{(i-1) % 100:05d} != {row.ZipCode}')
    if i != row.SecretKey:
        raise Exception(f'{i}: {i} != SecretKey={row.SecretKey}')
    if f'{i*2}' != row.FieldA:
        raise Exception(f'{i}: {i*2} != FieldA={row.FieldA}')

    if (NumSources < 2) or (i > NumBRecords):
        if row.FieldB is not None:
            raise Exception(
                "{i}: row.FieldB is not None, NumSources={NumSources}, NumBRecords={NumBRecords}")
    else:
        if f'{i*3}' != row.FieldB:
            raise Exception(f'{i}: {i*3} != FieldB={row.FieldB}')

    if NumSources < 3:
        if row.FieldC is not None:
            raise Exception(
                "{i}: row.FieldC is not None, NumSources={NumSources}")
    else:
        if f'{i*5}' != row.FieldC:
            raise Exception(f'{i}: {i*5} != FieldC={row.FieldC}')

    if NumSources < 4:
        if row.FieldD is not None:
            raise Exception(
                "{i}: row.FieldD is not None, NumSources={NumSources}")
    else:
        if f'{i*7}' != row.FieldD:
            raise Exception(f'{i}: {i*7} != FieldD={row.FieldD}')

    if NumSources < 5:
        if row.FieldE is not None:
            raise Exception(
                "{i}: row.FieldE is not None, NumSources={NumSources}")
    else:
        if f'{i*11}' != row.FieldE:
            raise Exception(f'{i}: {i*11} != FieldE={row.FieldE}')

    if NumSources < 6:
        if row.FieldF is not None:
            raise Exception(
                "{i}: row.FieldF is not None, NumSources={NumSources}")
    else:
        if f'{i*13}' != row.FieldF:
            raise Exception(f'{i}: {i*13} != FieldF={row.FieldF}')


def count_in_a_partition(
        idx: int,
        iterator: Iterable[Row]
) -> Iterable[tuple[int, int]]:
    yield idx, sum(1 for _ in iterator)


def print_partition_distribution(
        rdd_out: RDD[Row],
        df_out: PySparkDataFrame,
) -> None:
    print("records per partition ",
          (rdd_out or df_out.rdd)
          .mapPartitionsWithIndex(count_in_a_partition)
          .collect())
