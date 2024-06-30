#! python
# usage: cd src; python -m perf_test_runner_for_dask ; cd ..

from challenges.vanilla import vanilla_dask_runner

# from utils.tidy_spark_session import LOCAL_NUM_EXECUTORS


def main():
    # with DaskClient(
    #         processes=True,
    #         n_workers=LOCAL_NUM_EXECUTORS,
    #         threads_per_worker=1,
    # ) as dask_client:
    vanilla_dask_runner.do_with_client()


if __name__ == "__main__":
    main()
    print("Done!")
