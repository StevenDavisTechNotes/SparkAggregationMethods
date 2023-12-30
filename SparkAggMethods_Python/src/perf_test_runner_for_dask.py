#! python
# usage: python -m perf_test_runner_for_dask
from dask.distributed import Client as DaskClient

from challenges.vanilla import VanillaDaskRunner  # noqa: F401
from utils.TidySparkSession import LOCAL_NUM_EXECUTORS


def main():
    with DaskClient(
            processes=True,
            n_workers=LOCAL_NUM_EXECUTORS,
            threads_per_worker=1,
    ) as dask_client:
        VanillaDaskRunner.do_with_client(dask_client)


if __name__ == "__main__":
    main()
