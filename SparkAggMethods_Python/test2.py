# spark-submit --master local[8] --deploy-mode client --conf "spark.pyspark.python=.\venv\scripts\python.exe" 'test2.py'

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from datetime import datetime, date
from pyspark.sql import Row

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
print(df, df.count())

print("Done")
