import time
from typing import TextIO

import pandas as pd
from pyspark import RDD
from pyspark.sql import DataFrame as spark_DataFrame
from pyspark.sql import Row

from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data_using_pyspark import (
    PySparkDataSetWithAnswer, PysparkPythonTestMethod)
from six_field_test_data.six_run_result_types import write_run_result
from six_field_test_data.six_test_data_types import (ExecutionParameters,
                                                     RunResult)
from utils.tidy_spark_session import TidySparkSession


def test_one_step_in_itinerary(
        engine: CalcEngine,
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        test_method: PysparkPythonTestMethod,
        result_columns: list[str],
        file: TextIO,
        data_set: PySparkDataSetWithAnswer,
        correct_answer: pd.DataFrame
):
    def check_partitions(rdd: RDD):
        if rdd.getNumPartitions() > max(data_set.data.AggTgtNumPartitions, exec_params.DefaultParallelism):
            print(
                f"{test_method.strategy_name} output rdd has {rdd.getNumPartitions()} partitions")
            findings = rdd.collect()
            print(f"size={len(findings)}, ", findings)
            exit(1)

    startedTime = time.time()
    rdd_some: RDD
    match test_method.delegate(
            spark_session, exec_params, data_set):
        case spark_DataFrame() as spark_df:
            rdd_some = spark_df.rdd
            check_partitions(rdd_some)
            df_answer = spark_df.toPandas()
            finishedTime = time.time()
        case RDD() as rdd_some:
            check_partitions(rdd_some)
            answer = rdd_some.collect()
            finishedTime = time.time()
            if len(answer) == 0:
                df_answer = pd.DataFrame(data=[], columns=result_columns)
            else:
                match answer[0]:
                    case Row():
                        df_answer = pd.DataFrame.from_records([x.asDict() for x in answer])
                    case _:
                        df_answer = pd.DataFrame.from_records([x._asdict() for x in answer])
        case "infeasible":
            return
        case _:
            raise ValueError("Must return at least 1 type")
    if correct_answer is data_set.answer.bilevel_answer:
        if 'avg_var_of_E2' not in df_answer:
            df_answer['avg_var_of_E2'] = df_answer['avg_var_of_E']
    elif correct_answer is data_set.answer.conditional_answer:
        if 'cond_var_of_E2' not in df_answer:
            df_answer['cond_var_of_E2'] = df_answer['cond_var_of_E']
    elif correct_answer is data_set.answer.vanilla_answer:
        if 'var_of_E2' not in df_answer:
            df_answer['var_of_E2'] = df_answer['var_of_E']
    else:
        raise ValueError("Must return at least 1 type")
    abs_diff = float(
        (correct_answer - df_answer)
        .abs().max().max())
    status = abs_diff < 1e-12
    assert (status is True)
    recordCount = len(df_answer)
    result = RunResult(
        engine=engine,
        dataSize=data_set.description.NumDataPoints,
        elapsedTime=finishedTime - startedTime,
        recordCount=recordCount)
    write_run_result(test_method, result, file)
