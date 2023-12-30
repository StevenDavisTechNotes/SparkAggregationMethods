from typing import Callable, Iterable, Optional, Tuple, TypeVar, cast

from pyspark import RDD, StorageLevel

T = TypeVar('T')


def non_commutative_tree_aggregate(
        rdd1: RDD[T],
        zeroValueFactory: Callable[[], T],
        seqOp: Callable[[T, T], T],
        combOp: Callable[[T, T], T],
        depth: int = 2,
        divisionBase: int = 2,
        storage_level: Optional[StorageLevel] = None,
) -> RDD[T]:
    def aggregate_partition(
            ipart: int,
            iterator: Iterable[tuple[int, T]],
    ) -> Iterable[tuple[int, T]]:
        return full_aggregate_partition(ipart, iterator, zeroValueFactory, seqOp)

    def combine_in_partition(
            ipart: int,
            iterator: Iterable[tuple[tuple[int, int], tuple[int, T]]],
    ) -> Iterable[tuple[int, T]]:
        return full_combine_in_partition(ipart, iterator, zeroValueFactory, combOp)

    def process_rdd(rdd_loop: RDD[Tuple[int, T]], numRows: int, idepth: int,
                    divisionBase: int) -> RDD[Tuple[int, T]]:
        return full_process_rdd(rdd_loop, numRows, idepth, divisionBase, combine_in_partition)

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
    for idepth in range(depth - 1, -1, -1):
        rdd_loop = process_rdd(rdd_loop, numRows, idepth, divisionBase)
    rdd8: RDD[T] = rdd_loop \
        .map(lambda x: x[1])
    return rdd8


def full_aggregate_partition(
        ipart: int,
        iterator: Iterable[tuple[int, T]],
        zeroValueFactory: Callable[[], T],
        seqOp: Callable[[T, T], T],
) -> Iterable[tuple[int, T]]:
    acc = zeroValueFactory()
    lastindex = None
    for index, obj in iterator:
        acc = seqOp(acc, obj)
        lastindex = index
    if lastindex is not None:
        yield (lastindex, acc)


def full_combine_in_partition(
        ipart: int,
        iterator: Iterable[tuple[tuple[int, int], tuple[int, T]]],
        zeroValueFactory: Callable[[], T],
        combOp: Callable[[T, T], T],
) -> Iterable[tuple[int, T]]:
    acc = zeroValueFactory()
    lastindex = None
    for (_ipart, _subindex), (index, obj) in iterator:
        acc = combOp(acc, obj)
        lastindex = index
    if lastindex is not None:
        yield (lastindex, acc)


def full_process_rdd(
        rdd_loop: RDD[Tuple[int, T]],
        numRows: int,
        idepth: int,
        divisionBase: int,
        combineInPartition: Callable[[int, Iterable[Tuple[Tuple[int, int], Tuple[int, T]]]], Iterable[Tuple[int, T]]],
) -> RDD[Tuple[int, T]]:
    rdd_loop.localCheckpoint()
    numSegments = pow(divisionBase, idepth)
    if numSegments >= numRows:
        return rdd_loop
    segmentSize = (numRows + numSegments - 1) // numSegments
    rdd5: RDD[Tuple[Tuple[int, int], Tuple[int, T]]] = rdd_loop \
        .keyBy(lambda x: (x[0] // segmentSize, x[0] % segmentSize))
    rdd6 = cast(
        RDD[Tuple[Tuple[int, int], Tuple[int, T]]],
        rdd5
        .repartitionAndSortWithinPartitions(
            numPartitions=numSegments,
            partitionFunc=lambda x: x[0])  # type: ignore
    )
    rdd7: RDD[Tuple[int, T]] = rdd6 \
        .mapPartitionsWithIndex(combineInPartition)
    return rdd7
