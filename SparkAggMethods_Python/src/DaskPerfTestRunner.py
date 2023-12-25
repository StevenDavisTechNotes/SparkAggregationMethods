#! python
# usage: python -m DaskPerfTestRunner
from dask.distributed import Client as DaskClient

from Utils.TidySparkSession import LOCAL_NUM_EXECUTORS
from VanillaPerfTest import VanillaDaskRunner  # noqa: F401


def main():
    with DaskClient(
            processes=True,
            n_workers=LOCAL_NUM_EXECUTORS,
            threads_per_worker=1,
    ) as dask_client:
        VanillaDaskRunner.do_with_client(dask_client)


if __name__ == "__main__":
    main()
