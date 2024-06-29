from typing import Callable, Iterable, Optional, TypeVar, cast

from pyspark import RDD, StorageLevel

T = TypeVar('T')


def non_commutative_tree_aggregate(
        rdd1: RDD[T],
        zero_value_factory: Callable[[], T],
        seq_op: Callable[[T, T], T],
        comb_op: Callable[[T, T], T],
        depth: int = 2,
        division_base: int = 2,
        storage_level: Optional[StorageLevel] = None,
) -> RDD[T]:
    def aggregate_partition(
            i_part: int,
            iterator: Iterable[tuple[int, T]],
    ) -> Iterable[tuple[int, T]]:
        return full_aggregate_partition(i_part, iterator, zero_value_factory, seq_op)

    def combine_in_partition(
            i_part: int,
            iterator: Iterable[tuple[tuple[int, int], tuple[int, T]]],
    ) -> Iterable[tuple[int, T]]:
        return full_combine_in_partition(i_part, iterator, zero_value_factory, comb_op)

    def process_rdd(
            rdd_loop: RDD[tuple[int, T]],
            num_rows: int,
            i_depth: int,
            division_base: int,
    ) -> RDD[tuple[int, T]]:
        return full_process_rdd(rdd_loop, num_rows, i_depth, division_base, combine_in_partition)

    persistedRDD = rdd1
    if storage_level is not None:
        rdd1.persist(StorageLevel.DISK_ONLY)
    rdd2: RDD[tuple[T, int]] = rdd1.zipWithIndex()
    if storage_level is not None:
        rdd2.persist(StorageLevel.DISK_ONLY)
    numRows = rdd2.count()
    if storage_level is not None:
        persistedRDD.unpersist()
    persistedRDD = rdd2
    rdd3: RDD[tuple[int, T]] = rdd2.map(lambda x: (x[1], x[0]))
    rdd_loop: RDD[tuple[int, T]] = rdd3.mapPartitionsWithIndex(aggregate_partition)
    for i_depth in range(depth - 1, -1, -1):
        rdd_loop = process_rdd(rdd_loop, numRows, i_depth, division_base)
    rdd8: RDD[T] = rdd_loop \
        .map(lambda x: x[1])
    return rdd8


def full_aggregate_partition(
        i_part: int,
        iterator: Iterable[tuple[int, T]],
        zero_value_factory: Callable[[], T],
        seq_op: Callable[[T, T], T],
) -> Iterable[tuple[int, T]]:
    acc = zero_value_factory()
    last_index = None
    for index, obj in iterator:
        acc = seq_op(acc, obj)
        last_index = index
    if last_index is not None:
        yield (last_index, acc)


def full_combine_in_partition(
        i_part: int,
        iterator: Iterable[tuple[tuple[int, int], tuple[int, T]]],
        zero_value_factory: Callable[[], T],
        combOp: Callable[[T, T], T],
) -> Iterable[tuple[int, T]]:
    acc = zero_value_factory()
    last_index = None
    for (_i_part, _sub_index), (index, obj) in iterator:
        acc = combOp(acc, obj)
        last_index = index
    if last_index is not None:
        yield (last_index, acc)


def full_process_rdd(
        rdd_loop: RDD[tuple[int, T]],
        num_rows: int,
        i_depth: int,
        division_base: int,
        combine_in_partition: Callable[[int, Iterable[tuple[tuple[int, int], tuple[int, T]]]], Iterable[tuple[int, T]]],
) -> RDD[tuple[int, T]]:
    rdd_loop.localCheckpoint()
    num_segments = pow(division_base, i_depth)
    if num_segments >= num_rows:
        return rdd_loop
    segment_size = (num_rows + num_segments - 1) // num_segments
    rdd5: RDD[tuple[tuple[int, int], tuple[int, T]]] = rdd_loop \
        .keyBy(lambda x: (x[0] // segment_size, x[0] % segment_size))
    rdd6 = cast(
        RDD[tuple[tuple[int, int], tuple[int, T]]],
        rdd5
        .repartitionAndSortWithinPartitions(
            numPartitions=num_segments,
            partitionFunc=lambda x: x[0])  # type: ignore
    )
    rdd7: RDD[tuple[int, T]] = rdd6 \
        .mapPartitionsWithIndex(combine_in_partition)
    return rdd7
