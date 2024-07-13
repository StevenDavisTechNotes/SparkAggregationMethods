#! python
# usage: .\venv\Scripts\activate.ps1; python -m src.perf_test_runner_for_dask

from src.challenges.vanilla import vanilla_dask_runner

# from src.utils.tidy_spark_session import LOCAL_NUM_EXECUTORS


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
