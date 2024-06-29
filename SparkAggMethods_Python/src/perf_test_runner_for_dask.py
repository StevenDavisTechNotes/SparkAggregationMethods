#! python
# usage: cd src; python -m perf_test_runner_for_dask
from dask.distributed import Client as DaskClient

from challenges.vanilla import vanilla_dask_runner  # noqa: F401
from t_utils.tidy_spark_session import LOCAL_NUM_EXECUTORS


def main():
    with DaskClient(
            processes=True,
            n_workers=LOCAL_NUM_EXECUTORS,
            threads_per_worker=1,
    ) as dask_client:
        vanilla_dask_runner.do_with_client(dask_client)


if __name__ == "__main__":
    main()
