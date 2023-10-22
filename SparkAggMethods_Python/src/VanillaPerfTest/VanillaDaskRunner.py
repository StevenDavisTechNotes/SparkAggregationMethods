import argparse
import gc
import random
import time
from typing import List, NamedTuple, Optional, TextIO, Tuple

import pandas as pd
from dask.distributed import Client as DaskClient

from PerfTestCommon import CalcEngine
from SixFieldCommon.Dask_SixFieldTestData import (DaskDataSetWithAnswer,
                                                  DaskPythonTestMethod,
                                                  populate_data_set_dask)
from SixFieldCommon.SixFieldRunResult import write_header, write_run_result
from SixFieldCommon.SixFieldTestData import (
    SHARED_LOCAL_TEST_DATA_FILE_LOCATION, ExecutionParameters, RunResult)
from Utils.TidySparkSession import LOCAL_NUM_EXECUTORS
from Utils.Utils import always_true, set_random_seed
from VanillaPerfTest.VanillaDataTypes import result_columns
from VanillaPerfTest.VanillaDirectory import dask_implementation_list
from VanillaPerfTest.VanillaRunResult import dask_infeasible, run_log_file_path

ENGINE = CalcEngine.DASK
DEBUG_ARGS = None if False else (
    []
    # + '--size 10k'.split()
    + '--runs 1'.split()
    # + '--random-seed 1234'.split()
    + ['--no-shuffle']
    # + ['--strategy',
    #    'vanilla_sql',
    #    'vanilla_fluent',
    # 'vanilla_pandas',
    # 'vanilla_pandas_numpy',
    #    'vanilla_rdd_grpmap',
    # 'vanilla_rdd_reduce',
    # 'vanilla_rdd_mappart'
    #    ]
)


class Arguments(NamedTuple):
    num_runs: int
    random_seed: Optional[int]
    shuffle: bool
    sizes: List[str]
    strategies: List[str]
    exec_params: ExecutionParameters


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument('--random-seed', type=int)
    parser.add_argument('--runs', type=int, default=30)
    parser.add_argument(
        '--size',
        choices=['1', '10', '100', '1k', '10k', '100k'],
        default=['10', '100', '1k', '10k', '100k'],
        nargs="+")
    parser.add_argument(
        '--shuffle', default=True,
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '--strategy',
        choices=dask_implementation_list,
        default=[
            'da_vanilla_pandas',
        ],
        nargs="+")
    if DEBUG_ARGS is None:
        args = parser.parse_args()
    else:
        args = parser.parse_args(DEBUG_ARGS)
    return Arguments(
        num_runs=args.runs,
        random_seed=args.random_seed,
        shuffle=args.shuffle,
        sizes=args.size,
        strategies=args.strategy,
        exec_params=ExecutionParameters(
            DefaultParallelism=2 * LOCAL_NUM_EXECUTORS,
            TestDataFolderLocation=SHARED_LOCAL_TEST_DATA_FILE_LOCATION,
        ),
    )


def do_test_runs(
        args: Arguments,
        dask_client: DaskClient,
) -> None:
    data_sets = populate_data_sets(args, )
    keyed_implementation_list = {
        x.strategy_name: x for x in dask_implementation_list}
    itinerary: List[Tuple[DaskPythonTestMethod, DaskDataSetWithAnswer]] = [
        (test_method, data_set)
        for strategy in args.strategies
        if always_true(test_method := keyed_implementation_list[strategy])
        for data_set in data_sets
        if not dask_infeasible(strategy, data_set)
        for _ in range(0, args.num_runs)
    ]
    if args.random_seed is not None:
        set_random_seed(args.random_seed)
    if args.shuffle:
        random.shuffle(itinerary)
    with open(run_log_file_path(ENGINE), 'at+') as file:
        write_header(file)
        for index, (test_method, data_set) in enumerate(itinerary):
            print("Working on %d of %d" % (index, len(itinerary)))
            print(f"Working on {test_method.strategy_name} for {data_set.description.SizeCode}")
            test_one_step_in_itinerary(dask_client, args.exec_params, test_method, file, data_set)
            gc.collect()
            time.sleep(0.1)


def populate_data_sets(
        args: Arguments,
) -> List[DaskDataSetWithAnswer]:

    def generate_single_test_data_set_simple(
            code: str,
            num_grp_1:
            int, num_grp_2: int,
            num_data_points: int
    ) -> DaskDataSetWithAnswer:
        return populate_data_set_dask(
            args.exec_params,
            code, num_grp_1, num_grp_2, num_data_points)

    data_sets = [x for x in [
        generate_single_test_data_set_simple('1', 3, 3, 10 ** 0,
                                             ) if '1' in args.sizes else None,
        generate_single_test_data_set_simple('10', 3, 3, 10 ** 1,
                                             ) if '10' in args.sizes else None,
        generate_single_test_data_set_simple('100', 3, 3, 10 ** 2,
                                             ) if '100' in args.sizes else None,
        generate_single_test_data_set_simple('1k', 3, 3, 10 ** 3,
                                             ) if '1k' in args.sizes else None,
        generate_single_test_data_set_simple('10k', 3, 3, 10 ** 4,
                                             ) if '10k' in args.sizes else None,
        generate_single_test_data_set_simple('100k', 3, 3, 10 ** 5,
                                             ) if '100k' in args.sizes else None,
        generate_single_test_data_set_simple('1m', 3, 3, 10 ** 6,
                                             ) if '1m' in args.sizes else None,
    ] if x is not None]

    return data_sets


def test_one_step_in_itinerary(
        dask_client: DaskClient,
        exec_params: ExecutionParameters,
        test_method: DaskPythonTestMethod,
        file: TextIO,
        data_set: DaskDataSetWithAnswer,
):
    startedTime = time.time()
    bag, ddf, pdf = test_method.delegate(
        dask_client, exec_params, data_set)
    if bag is not None:
        if bag.npartitions > max(data_set.data.AggTgtNumPartitions, exec_params.DefaultParallelism):
            print(
                f"{test_method.strategy_name} output rdd has {bag.npartitions} partitions")
            findings = bag.compute()
            print(f"size={len(findings)}, ", findings)
            exit(1)
        lst_answer = bag.compute()
        finishedTime = time.time()
        if len(lst_answer) > 0:
            df_answer = pd.DataFrame.from_records([x.asDict() for x in lst_answer])
        else:
            df_answer = pd.DataFrame(columns=result_columns)
    elif ddf is not None:
        if ddf.npartitions > max(data_set.data.AggTgtNumPartitions, exec_params.DefaultParallelism):
            print(
                f"{test_method.strategy_name} output rdd has {ddf.npartitions} partitions")
            findings = ddf.compute()
            print(f"size={len(findings)}, ", findings)
            exit(1)
        df_answer = ddf.compute()
        finishedTime = time.time()
    elif pdf is not None:
        df_answer = pdf
        finishedTime = time.time()
    else:
        raise ValueError("No result returned")
    if 'var_of_E2' not in df_answer:
        df_answer['var_of_E2'] = df_answer['var_of_E']
    abs_diff = float(
        (data_set.answer.vanilla_answer - df_answer)
        .abs().max().max())
    status = abs_diff < 1e-12
    assert (status is True)
    recordCount = len(df_answer)
    result = RunResult(
        engine=ENGINE,
        dataSize=data_set.description.NumDataPoints,
        elapsedTime=finishedTime - startedTime,
        recordCount=recordCount)
    write_run_result(test_method, result, file)


def main():
    args = parse_args()
    with DaskClient(
            processes=False,
            asynchronous=False,
            n_workers=LOCAL_NUM_EXECUTORS,
            threads_per_worker=1,
    ) as dask_client:
        do_test_runs(args, dask_client)


if __name__ == "__main__":
    main()
