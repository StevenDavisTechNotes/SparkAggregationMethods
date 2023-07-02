import collections
import hashlib
import os
import random
from dataclasses import dataclass
from typing import List, Optional

import pyspark.sql.functions as func
import pyspark.sql.types as DataTypes
from pyspark.sql import SparkSession


DataPoint = collections.namedtuple("DataPoint",
                                   ["id", "grp", "subgrp", "A", "B", "C", "D", "E", "F"])
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

@dataclass(frozen=True)
class DataTuple:
    relCard: int
    data: List[DataPoint]

def generateData(numGrp1=3, numGrp2=3, repetition=1000) -> DataTuple:
    data= [
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
    return DataTuple(numGrp2 // numGrp1, data)
