from typing import Callable, Iterable, Optional, Tuple, TypeVar, cast

from pyspark import RDD, StorageLevel

T = TypeVar('T')


def nonCommutativeTreeAggregate(
        rdd1: RDD[T],
        zeroValueFactory: Callable[[], T],
        seqOp: Callable[[T, T], T],
        combOp: Callable[[T, T], T],
        depth: int = 2,
        divisionBase: int = 2,
        storage_level: Optional[StorageLevel] = None,
) -> RDD[T]:
    def aggregatePartition(
            ipart: int,
            iterator: Iterable[Tuple[int, T]],
    ) -> Iterable[Tuple[int, T]]:
        acc = zeroValueFactory()
        lastindex = None
        for index, obj in iterator:
            acc = seqOp(acc, obj)
            lastindex = index
        if lastindex is not None:
            yield (lastindex, acc)

    def combineInPartition(
            ipart: int,
            iterator: Iterable[Tuple[Tuple[int, int], Tuple[int, T]]],
    ) -> Iterable[Tuple[int, T]]:
        acc = zeroValueFactory()
        lastindex = None
        for (ipart, subindex), (index, obj) in iterator:
            acc = combOp(acc, obj)
            lastindex = index
        if lastindex is not None:
            yield (lastindex, acc)

    persistedRDD = rdd1
    if storage_level is not None:
        rdd1.persist(StorageLevel.DISK_ONLY)
    rdd2: RDD[Tuple[T, int]] = rdd1.zipWithIndex()
    if storage_level is not None:
        rdd2.persist(StorageLevel.DISK_ONLY)
    numRows = rdd2.count()
    if storage_level is not None:
        persistedRDD.unpersist()
    persistedRDD = rdd2
    rdd3: RDD[Tuple[int, T]] = rdd2.map(lambda x: (x[1], x[0]))
    rdd_loop: RDD[Tuple[int, T]] = rdd3.mapPartitionsWithIndex(aggregatePartition)
    for idepth in range(depth - 1, -1, -1):
        rdd_loop.localCheckpoint()
        numSegments = pow(divisionBase, idepth)
        if numSegments >= numRows:
            continue
        segmentSize = (numRows + numSegments - 1) // numSegments
        rdd5: RDD[Tuple[Tuple[int, int], Tuple[int, T]]] = rdd_loop \
            .keyBy(lambda x: (x[0] // segmentSize, x[0] % segmentSize))
        rdd6 = rdd5 \
            .repartitionAndSortWithinPartitions(
                numPartitions=numSegments,
                partitionFunc=lambda x: x[0])  # type: ignore
        rdd7: RDD[Tuple[int, T]] = rdd6 \
            .mapPartitionsWithIndex(combineInPartition)
        rdd_loop = rdd7
    rdd8: RDD[T] = rdd_loop \
        .map(lambda x: cast(T, x[1]))
    return rdd8
