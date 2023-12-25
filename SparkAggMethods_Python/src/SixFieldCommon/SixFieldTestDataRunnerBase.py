import time
from typing import Optional, TextIO

import pandas as pd
from pyspark import RDD
from pyspark.sql import Row

from PerfTestCommon import CalcEngine
from SixFieldCommon.PySpark_SixFieldTestData import (
    PySparkDataSetWithAnswer, PysparkPythonPendingAnswerSet,
    PysparkPythonTestMethod)
from SixFieldCommon.SixFieldRunResult import write_run_result
from SixFieldCommon.SixFieldTestData import ExecutionParameters, RunResult
from Utils.TidySparkSession import TidySparkSession


def to_some_rdd(result: PysparkPythonPendingAnswerSet) -> Optional[RDD]:
    return (
        result.rdd_tuple or result.rdd_row or
        (result.spark_df.rdd if result.spark_df is not None else None)
    )


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
    startedTime = time.time()
    pending_result = test_method.delegate(
        spark_session, exec_params, data_set)
    rdd_some = to_some_rdd(pending_result)
    if rdd_some is None:
        raise ValueError("Must return at least 1 type")
    if rdd_some.getNumPartitions() > max(data_set.data.AggTgtNumPartitions, exec_params.DefaultParallelism):
        print(
            f"{test_method.strategy_name} output rdd has {rdd_some.getNumPartitions()} partitions")
        findings = rdd_some.collect()
        print(f"size={len(findings)}, ", findings)
        exit(1)
    match pending_result:
        case PysparkPythonPendingAnswerSet(rdd_tuple=rdd_tuple) if rdd_tuple is not None:
            answer = rdd_tuple.collect()
            finishedTime = time.time()
            if len(answer) > 0:
                df_answer = pd.DataFrame.from_records([x._asdict() for x in answer])
            else:
                df_answer = pd.DataFrame(columns=result_columns)
        case PysparkPythonPendingAnswerSet(spark_df=spark_df) if spark_df is not None:
            df_answer = spark_df.toPandas()
            finishedTime = time.time()
        case PysparkPythonPendingAnswerSet(rdd_row=rdd_row) if rdd_row is not None:
            answer = rdd_row.collect()
            finishedTime = time.time()
            if len(answer) > 0:
                assert isinstance(answer[0], Row)
                df_answer = pd.DataFrame.from_records([x.asDict() for x in answer])
            else:
                df_answer = pd.DataFrame(columns=result_columns)
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
