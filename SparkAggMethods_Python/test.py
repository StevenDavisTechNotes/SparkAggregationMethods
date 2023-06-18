# spark-submit --master local[8] --driver-memory 1g --deploy-mode client --conf spark.pyspark.virtualenv.enabled=true  --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.requirements=requirements.txt --conf spark.pyspark.virtualenv.bin.path=venv --conf spark.pyspark.python="$pwd\venv\scripts\python.exe" 'test.py'
# spark-submit --master local[8] --driver-memory 1g --deploy-mode client --conf spark.pyspark.python=".\venv\scripts\python.exe" 'test.py'

import os
import random

import findspark
from pyspark.sql import SparkSession

full_path_to_python = os.path.join(
    os.getcwd(), "venv", "scripts", "python.exe")
os.environ["PYSPARK_PYTHON"] = full_path_to_python
os.environ["PYSPARK_DRIVER_PYTHON"] = full_path_to_python

findspark.init()
spark = SparkSession \
    .builder \
    .appName("VanillaPerfTest") \
    .config("spark.pyspark.virtualenv.enabled", True) \
    .config("spark.pyspark.virtualenv.type", "native") \
    .config("spark.pyspark.virtualenv.requirements", "requirements.txt") \
    .config("spark.pyspark.virtualenv.bin.path", "venv") \
    .config("spark.pyspark.python", full_path_to_python) \
    .getOrCreate()
sc = spark.sparkContext

num_samples = 1000000


def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1


count = sc.parallelize(range(0, num_samples)).filter(inside).count()
pi = 4 * count / num_samples
print(pi)
sc.stop()
