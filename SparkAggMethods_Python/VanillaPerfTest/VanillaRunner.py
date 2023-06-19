#!python
# python -m VanillaPerfTest.VanillaRunner   
from typing import List, Tuple

import gc
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession

from PerfTestCommon import PythonTestMethod, count_iter
from Utils.SparkUtils import createSparkContext, setupSparkContext

from .Strategy.Directory import implementation_list
from .RunResult import RunResult
from .VanillaTestData import DataPointAsTuple, generateData

RESULT_FILE_PATH = 'Results/vanilla_runs.csv'


def DoTesting(spark: SparkSession, sc: SparkContext, log):
    pyData_3_3_1 = generateData(3, 3, 10**0)
    pyData_3_3_10 = generateData(3, 3, 10**1)
    pyData_3_3_100 = generateData(3, 3, 10**2)
    # pyData_3_3_1k = generateData(3, 3, 10**3)
    # pyData_3_3_10k = generateData(3, 3, 10**4)
    # pyData_3_3_100k = generateData(3, 3, 10**5)
    # pyData_3_3_1m = generateData(3,3,10**6)
    NumRunsPer = 1  # 30  # 100
    cond_run_itinerary: List[Tuple[PythonTestMethod, List[DataPointAsTuple]]] = []
    for cond_method in implementation_list:
        for data in [pyData_3_3_10, pyData_3_3_100,
                     #  pyData_3_3_1k, pyData_3_3_10k, pyData_3_3_100k
                     ]:
            cond_run_itinerary.extend(
                (cond_method, data)
                for _i in range(0, NumRunsPer))
    # random.shuffle(cond_run_itinerary)
    # vanilla_panda_cupy(pyData_3_3_1) # for code generation
    with open(RESULT_FILE_PATH, 'a') as f:
        for index, (cond_method, data) in enumerate(cond_run_itinerary):
            log.info("Working on %d of %d" % (index, len(cond_run_itinerary)))
            startedTime = time.time()
            rdd, df = cond_method.delegate(spark, data)
            if df is not None:
                rdd = df.rdd
            assert rdd is not None
            recordCount = count_iter(rdd.toLocalIterator())
            finishedTime = time.time()
            result = RunResult(
                dataSize=len(data),
                elapsedTime=finishedTime-startedTime,
                recordCount=recordCount)
            f.write("%s,%s,%d,%f,%d\n" % (cond_method.name, cond_method.interface,
                    result.dataSize, result.elapsedTime, result.recordCount))
            f.flush()
            df = None
            rdd = None
            gc.collect()


if __name__ == "__main__":
    spark = createSparkContext({
        "spark.sql.shuffle.partitions": 7,
        "spark.ui.enabled": "false",
        "spark.rdd.compress": "false",
        "spark.driver.memory": "2g",
        "spark.executor.memory": "3g",
        "spark.executor.memoryOverhead": "1g",
    })
    sc, log = setupSparkContext(spark)
    DoTesting(spark, sc, log)
