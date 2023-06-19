from typing import List

from dataclasses import dataclass
import random


import pyspark.sql.types as DataTypes


@dataclass(frozen=True)
class DataPoint():
    id: int
    grp: int
    subgrp: int
    A: int
    B: int
    C: float
    D: float
    E: float
    F: float


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


groupby_columns = ['grp', 'subgrp']
agg_columns_non_null = ['mean_of_C', 'max_of_D']
agg_columns_nullable = ['var_of_E', 'var_of_E2']
agg_columns = agg_columns_non_null+agg_columns_nullable
postAggSchema = DataTypes.StructType(
    [x for x in DataPointSchema.fields if x.name in groupby_columns]
    + [DataTypes.StructField(name, DataTypes.DoubleType(), False)
        for name in agg_columns_non_null]
    + [DataTypes.StructField(name, DataTypes.DoubleType(), True)
        for name in agg_columns_nullable])

def generateData(numGrp1=3, numGrp2=3, repetition=1000) -> List[DataPoint]:
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
