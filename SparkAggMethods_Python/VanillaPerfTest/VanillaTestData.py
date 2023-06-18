from typing import List, cast

from dataclasses import dataclass
import random
import collections


import pyspark.sql.types as DataTypes


@dataclass(frozen=True)
class DataPoint:
    id: int
    grp: int
    subgrp: int
    A: int
    B: int
    C: float
    D: float
    E: float
    F: float


DataPointAsTuple = collections.namedtuple("DataPoint",
                                          ["id", "grp", "subgrp", "A", "B", "C", "D", "E", "F"])


def cast_data_points_to_tuples(src: List[DataPoint]) -> List[DataPointAsTuple]:
    return cast(List[DataPointAsTuple], src)


# DataPoint = collections.namedtuple("DataPoint",
#                                    ["id", "grp", "subgrp", "A", "B", "C", "D", "E", "F"])
DataPointSchema = DataTypes.StructType([
    DataTypes.StructField('id', DataTypes.LongType(), False),
    DataTypes.StructField('grp', DataTypes.LongType(), False),
    DataTypes.StructField('subgrp', DataTypes.LongType(), False),
    DataTypes.StructField('A', DataTypes.LongType(), False),
    DataTypes.StructField('B', DataTypes.LongType(), False),
    DataTypes.StructField('C', DataTypes.DoubleType(), False),
    DataTypes.StructField('D', DataTypes.DoubleType(), False),
    DataTypes.StructField('E', DataTypes.DoubleType(), False),
    DataTypes.StructField('F', DataTypes.DoubleType(), False)])


def generateData(numGrp1=3, numGrp2=3, repetition=1000):
    return [
        DataPoint(
            id=i,
            grp=(i // numGrp2) % numGrp1,
            subgrp=i % numGrp2,
            A=random.randint(1, repetition),
            B=random.randint(1, repetition),
            C=random.uniform(1, 10),
            D=random.uniform(1, 10),
            E=random.normalvariate(0, 10),
            F=random.normalvariate(1, 10))
        for i in range(0, numGrp1 * numGrp2 * repetition)]
