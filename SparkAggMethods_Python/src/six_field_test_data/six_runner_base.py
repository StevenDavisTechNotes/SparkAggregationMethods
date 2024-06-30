import time
from typing import TextIO

import pandas as pd
from dask.bag.core import Bag as DaskBag
from dask.dataframe.core import DataFrame as DaskDataFrame
from pyspark import RDD
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import Row

from challenges.vanilla.vanilla_test_data_types import result_columns
from perf_test_common import CalcEngine
from six_field_test_data.six_generate_test_data import (
    ChallengeMethodPythonDaskRegistration,
    ChallengeMethodPythonOnlyRegistration,
    ChallengeMethodPythonPysparkRegistration, DataSetDaskWithAnswer,
    DataSetPysparkWithAnswer, DataSetPythonOnlyWithAnswer)
from six_field_test_data.six_run_result_types import write_run_result
from six_field_test_data.six_test_data_types import (Challenge, DataSetAnswer,
                                                     DataSetDescription,
                                                     ExecutionParameters,
                                                     RunResult)
from utils.tidy_spark_session import TidySparkSession


def test_one_step_in_dask_itinerary(
        challenge: Challenge,
        exec_params: ExecutionParameters,
        challenge_method_registration: ChallengeMethodPythonDaskRegistration,
        file: TextIO,
        data_set: DataSetDaskWithAnswer,
):
    startedTime = time.time()
    df_answer: pd.DataFrame
    match challenge_method_registration.delegate(
        exec_params=exec_params,
        data_set=data_set,
    ):
        case DaskBag() as bag:
            if bag.npartitions > max(data_set.data.agg_tgt_num_partitions, exec_params.DefaultParallelism):
                print(
                    f"{challenge_method_registration.strategy_name} output rdd has {bag.npartitions} partitions")
                findings = bag.compute()
                print(f"size={len(findings)}, ", findings)
                exit(1)
            lst_answer = bag.compute()
            finishedTime = time.time()
            if len(lst_answer) > 0:
                df_answer = pd.DataFrame.from_records([x.asDict() for x in lst_answer])
            else:
                df_answer = pd.DataFrame(columns=result_columns)
        case DaskDataFrame() as ddf:
            if ddf.npartitions > max(data_set.data.agg_tgt_num_partitions, exec_params.DefaultParallelism):
                print(
                    f"{challenge_method_registration.strategy_name} output rdd has {ddf.npartitions} partitions")
                findings = ddf.compute()
                print(f"size={len(findings)}, ", findings)
                exit(1)
            df_answer = ddf.compute()
            finishedTime = time.time()
        case pd.DataFrame() as df_answer:
            finishedTime = time.time()
        case "infeasible":
            return
        case _:
            raise ValueError("No result returned")
    result = process_answer(
        engine=CalcEngine.DASK,
        challenge=challenge,
        data_set_description=data_set.description,
        correct_answer=data_set.answer.answer_for_challenge(challenge),
        startedTime=startedTime,
        df_answer=df_answer,
        finishedTime=finishedTime,
    )
    write_run_result(challenge_method_registration, result, file)


def test_one_step_in_pyspark_itinerary(
        challenge: Challenge,
        spark_session: TidySparkSession,
        exec_params: ExecutionParameters,
        challenge_method_registration: ChallengeMethodPythonPysparkRegistration,
        result_columns: list[str],
        file: TextIO,
        data_set: DataSetPysparkWithAnswer,
):
    def check_partitions(rdd: RDD):
        if rdd.getNumPartitions() > max(data_set.data.AggTgtNumPartitions, exec_params.DefaultParallelism):
            print(
                f"{challenge_method_registration.strategy_name} output rdd has {rdd.getNumPartitions()} partitions")
            findings = rdd.collect()
            print(f"size={len(findings)}, ", findings)
            exit(1)

    startedTime = time.time()
    rdd_some: RDD
    match challenge_method_registration.delegate(
            spark_session=spark_session,
            exec_params=exec_params,
            data_set=data_set,
    ):
        case PySparkDataFrame() as spark_df:
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
    result = process_answer(
        engine=CalcEngine.PYSPARK,
        challenge=challenge,
        data_set_description=data_set.description,
        correct_answer=data_set.answer.answer_for_challenge(challenge),
        startedTime=startedTime,
        df_answer=df_answer,
        finishedTime=finishedTime,
    )
    write_run_result(challenge_method_registration, result, file)


def test_one_step_in_python_only_itinerary(
        challenge: Challenge,
        exec_params: ExecutionParameters,
        challenge_method_registration: ChallengeMethodPythonOnlyRegistration,
        file: TextIO,
        data_set: DataSetPythonOnlyWithAnswer,
        correct_answer: DataSetAnswer,
):
    startedTime = time.time()
    match challenge_method_registration.delegate(
            exec_params=exec_params,
            data_set=data_set,
    ):
        case pd.DataFrame() as pandas_df:
            df_answer = pandas_df
            finishedTime = time.time()
        case "infeasible":
            return
        case _:
            raise ValueError("Must return at least 1 type")
    result = process_answer(
        engine=CalcEngine.PYTHON_ONLY,
        challenge=challenge,
        data_set_description=data_set.description,
        correct_answer=data_set.answer.answer_for_challenge(challenge),
        startedTime=startedTime,
        df_answer=df_answer,
        finishedTime=finishedTime,
    )
    write_run_result(challenge_method_registration, result, file)


def process_answer(
        engine: CalcEngine,
        challenge: Challenge,
        data_set_description: DataSetDescription,
        correct_answer: pd.DataFrame,
        startedTime: float,
        df_answer: pd.DataFrame,
        finishedTime: float,
):
    if challenge == Challenge.BI_LEVEL:
        if 'avg_var_of_E2' not in df_answer:
            df_answer['avg_var_of_E2'] = df_answer['avg_var_of_E']
    elif challenge == Challenge.CONDITIONAL:
        if 'cond_var_of_E2' not in df_answer:
            df_answer['cond_var_of_E2'] = df_answer['cond_var_of_E']
    elif challenge == Challenge.VANILLA:
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
        dataSize=data_set_description.NumDataPoints,
        elapsedTime=finishedTime - startedTime,
        recordCount=recordCount)
    return result
