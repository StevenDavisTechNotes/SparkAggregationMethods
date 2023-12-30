from typing import Iterable, List, Tuple

from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from challenges.deduplication.DedupeDataTypes import (ItineraryItem,
                                                      RecordSparseStruct)
from challenges.deduplication.DedupeTestData import nameHash


def arrangeFieldOrder(
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


def verifyCorrectness(
        itinerary_items: ItineraryItem,
        lst: List[Row],
):
    try:
        lst.sort(key=lambda x: int(x.FieldA))
        actualNumPeople = itinerary_items.data_set.num_people
        NumSources = itinerary_items.data_set.num_sources
        secretKeys = {x.SecretKey for x in lst}
        expectedSecretKeys = set(range(1, actualNumPeople + 1))
        if secretKeys != expectedSecretKeys:
            dmissing = expectedSecretKeys - secretKeys
            dextra = secretKeys - expectedSecretKeys
            raise Exception(f"Missing {dmissing} extra {dextra}")

        count = len(lst)
        if count != actualNumPeople:
            raise Exception(
                f"df.count()({count}) != numPeople({actualNumPeople}) ")
        NumBRecords = max(1, 2 * actualNumPeople // 100)
        for index, row in enumerate(lst):
            verify_single_line(NumSources, NumBRecords, index, row)
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
    if f"FFFFFFA{i}_{nameHash(i)}" != row.FirstName:
        raise Exception(
            f"{i}: FFFFFFA{i}_{nameHash(i)} != {row.FirstName}")
    if f'LLLLLLA{i}_{nameHash(i)}' != row.LastName:
        raise Exception(
            f'{i}: LLLLLLA{i}_{nameHash(i)} != {row.LastName}')
    if f'{i} Main St' != row.StreetAddress:
        raise Exception(f'{i} Main St != row.StreetAddress')
    if 'Plaineville ME' != row.City:
        raise Exception('{i}: Plaineville ME != {row.City}')
    if f'{(i-1)%100:05d}' != row.ZipCode:
        raise Exception(f'{(i-1)%100:05d} != {row.ZipCode}')
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
) -> Iterable[Tuple[int, int]]:
    yield idx, sum(1 for _ in iterator)


def printPartitionDistribution(
        rddout: RDD[Row],
        dfout: spark_DataFrame,
) -> None:
    print("records per partition ",
          (rddout or dfout.rdd)
          .mapPartitionsWithIndex(count_in_a_partition)
          .collect())
