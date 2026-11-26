# cSpell: ignore Plaineville, FFFFFFA, LLLLLLA
from collections.abc import Iterable
from typing import cast

from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row
from spark_agg_methods_common_python.challenges.dedupe.dedupe_test_data_types import DedupeDataSetDescription, name_hash
from src.challenges.dedupe.dedupe_test_data_types_pyspark import RecordSparseStruct
from src.utils.tidy_session_pyspark import TidySparkSession


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
        data_description: DedupeDataSetDescription,
        lst: list[Row],
        spark_session: TidySparkSession,
) -> bool:
    logger = spark_session.logger
    try:
        lst.sort(key=lambda x: int(x.FieldA))
        actual_num_people = data_description.num_people
        num_sources = data_description.num_sources
        secret_keys = {x.SecretKey for x in lst}
        expected_secret_keys = set(range(1, actual_num_people + 1))
        if secret_keys != expected_secret_keys:
            missing_keys = expected_secret_keys - secret_keys
            extra_keys = secret_keys - expected_secret_keys
            raise ValueError(f"Missing {missing_keys} extra {extra_keys}")

        count = len(lst)
        if count != actual_num_people:
            raise ValueError(
                f"df.count()({count}) != numPeople({actual_num_people}) ")
        NumBRecords = max(1, 2 * actual_num_people // 100)
        for index, row in enumerate(lst):
            verify_single_line(
                NumSources=num_sources,
                NumBRecords=NumBRecords,
                index=index,
                row=row,
            )
    except ValueError as exception:
        logger.info(f"data error {exception}")
        return False
    return True


def verify_single_line(
        NumSources: int,
        NumBRecords: int,
        index: int,
        row: Row,
) -> None:
    i = index + 1
    verify_first_name(
        row=row,
        i=i,
    )
    verify_last_name(
        row=row,
        i=i,
    )
    verify_street_address(
        row=row,
        i=i,
    )
    verify_city(
        row=row,
        i=i,
    )
    verify_zip_code(
        row=row,
        i=i,
    )
    verify_secret_key(
        row=row,
        i=i,
    )
    verify_column_a(
        row=row,
        i=i,
    )
    verify_column_b(
        num_sources=NumSources,
        num_b_records=NumBRecords,
        row=row,
        i=i,
    )
    verify_column_c(
        num_sources=NumSources,
        row=row,
        i=i,
    )
    verify_column_d(
        num_sources=NumSources,
        row=row,
        i=i,
    )
    verify_column_e(
        num_sources=NumSources,
        row=row,
        i=i,
    )
    verify_column_f(
        num_sources=NumSources,
        row=row,
        i=i,
    )


def verify_first_name(row: Row, i: int) -> None:
    if f"FFFFFFA{i}_{name_hash(i)}" != row.FirstName:
        raise ValueError(f"{i}: FFFFFFA{i}_{name_hash(i)} != {row.FirstName}")


def verify_last_name(row: Row, i: int) -> None:
    if f"LLLLLLA{i}_{name_hash(i)}" != row.LastName:
        raise ValueError(f"{i}: LLLLLLA{i}_{name_hash(i)} != {row.LastName}")


def verify_street_address(row: Row, i: int) -> None:
    if f"{i} Main St" != row.StreetAddress:
        raise ValueError(f"{i} Main St != StreetAddress={row.StreetAddress}")


def verify_city(row: Row, i: int) -> None:
    if "Plaineville ME" != row.City:
        raise ValueError(f"{i}: Plaineville ME != {row.City}")


def verify_zip_code(row: Row, i: int) -> None:
    if f"{(i-1) % 100:05d}" != row.ZipCode:
        raise ValueError(f"{(i-1) % 100:05d} != {row.ZipCode}")


def verify_secret_key(row: Row, i: int) -> None:
    if i != row.SecretKey:
        raise ValueError(f"{i}: {i} != SecretKey={row.SecretKey}")


def verify_column_a(row: Row, i: int) -> None:
    if f"{i*2}" != row.FieldA:
        raise ValueError(f"{i}: {i*2} != FieldA={row.FieldA}")


def verify_column_b(num_sources: int, num_b_records: int, row: Row, i: int) -> None:
    if (num_sources < 2) or (i > num_b_records):
        if row.FieldB is not None:
            raise ValueError(
                f"{i}: row.FieldB is not None, num_sources={num_sources}, num_b_records={num_b_records}")
    else:
        if f"{i*3}" != row.FieldB:
            raise ValueError(f"{i}: {i*3} != FieldB={row.FieldB}")


def verify_column_c(num_sources: int, row: Row, i: int) -> None:
    if num_sources < 3:
        if row.FieldC is not None:
            raise ValueError(
                f"{i}: row.FieldC is not None, num_sources={num_sources}")
    else:
        if f"{i*5}" != row.FieldC:
            raise ValueError(f"{i}: {i*5} != FieldC={row.FieldC}")


def verify_column_d(num_sources: int, row: Row, i: int) -> None:
    if num_sources < 4:
        if row.FieldD is not None:
            raise ValueError(
                f"{i}: row.FieldD is not None, num_sources={num_sources}")
    else:
        if f"{i*7}" != row.FieldD:
            raise ValueError(f"{i}: {i*7} != FieldD={row.FieldD}")


def verify_column_e(num_sources: int, row: Row, i: int) -> None:
    if num_sources < 5:
        if row.FieldE is not None:
            raise ValueError(
                f"{i}: row.FieldE is not None, num_sources={num_sources}")
    else:
        if f"{i*11}" != row.FieldE:
            raise ValueError(f"{i}: {i*11} != FieldE={row.FieldE}")


def verify_column_f(num_sources: int, row: Row, i: int) -> None:
    if num_sources < 6:
        if row.FieldF is not None:
            raise ValueError(
                f"{i}: row.FieldF is not None, num_sources={num_sources}")
    else:
        if f"{i*13}" != row.FieldF:
            raise ValueError(f"{i}: {i*13} != FieldF={row.FieldF}")


def count_in_a_partition(
        idx: int,
        iterator: Iterable[Row]
) -> Iterable[tuple[int, int]]:
    yield idx, sum(1 for _ in iterator)


def print_partition_distribution(
        rdd_out: RDD[Row],
        out_df: PySparkDataFrame,
        start_session: TidySparkSession,
) -> None:
    partition_counts = (
        (rdd_out or cast(RDD[Row], out_df.rdd))
        .mapPartitionsWithIndex(count_in_a_partition)
        .collect()
    )
    start_session.logger.info(f"records per partition {partition_counts}")
