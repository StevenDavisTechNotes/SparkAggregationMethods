from dataclasses import dataclass

import pyspark.sql.types as DataTypes
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from Utils.SparkUtils import TidySparkSession

from .DedupeDirectory import PythonTestMethod
from .DedupeTestData import RecordSparseStruct, nameHash


@dataclass(frozen=True)
class ItineraryItem:
    testMethod: PythonTestMethod
    NumSources: int
    numPeople: int
    dataSize: int
    df: spark_DataFrame


def arrangeFieldOrder(rec):
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
#


def verifyCorrectnessRdd(spark_session: TidySparkSession, itinerary_items: ItineraryItem, rdd: RDD[Row]):
    rdd = rdd \
        .map(arrangeFieldOrder)
    df = spark_session.spark.createDataFrame(rdd, schema=RecordSparseStruct)
    return verifyCorrectnessDf(spark_session, itinerary_items, df)
#


def verifyCorrectnessDf(spark_session: TidySparkSession, itinerary_items: ItineraryItem, df: spark_DataFrame):
    df = df.orderBy(df.FieldA.cast(DataTypes.IntegerType()))
    df.cache()

    try:
        actualNumPeople = itinerary_items.numPeople
        NumSources = itinerary_items.NumSources
        secretKeys = set(x.SecretKey for x in df.select(
            df.SecretKey).collect())
        expectedSecretKeys = set(range(1, actualNumPeople+1))
        if secretKeys != expectedSecretKeys:
            dmissing = expectedSecretKeys - secretKeys
            dextra = secretKeys - expectedSecretKeys
            raise Exception(f"Missing {dmissing} extra {dextra}")

        count = df.count()
        if count != actualNumPeople:
            raise Exception(
                f"df.count()({count}) != numPeople({actualNumPeople}) ")
        NumBRecords = max(1, 2*actualNumPeople//100)
        for index, row in enumerate(df.toLocalIterator()):
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
        # for
    except Exception as exception:
        spark_session.log.exception(exception)
        print("data error")
        return False

    print("Looking Good!")
    return True


def count_in_a_partition(idx, iterator):
    yield idx, sum(1 for _ in iterator)


def printPartitionDistribution(rddout, dfout):
    print("records per partition ",
          (rddout or dfout.rdd)
          .mapPartitionsWithIndex(count_in_a_partition)
          .collect())
