from distributed import Client, LocalCluster
from spark_agg_methods_common_python.challenges.six_field_test_data.six_test_data_types import (
    SixTestExecutionParameters,
)

_cluster: LocalCluster | None = None
_client: Client | None = None


def get_dask_client(exec_params: SixTestExecutionParameters) -> Client:
    """Lazily start one process-wide LocalCluster; the client is NOT the default client.

    Strategies that do not need a cluster keep using Dask's default threaded scheduler.
    """
    global _cluster, _client
    if _client is None:
        _cluster = LocalCluster(n_workers=exec_params.num_executors, threads_per_worker=1, processes=True)
        _client = Client(_cluster, set_as_default=False)
    return _client


def shutdown_dask_client() -> None:
    """Close the client and cluster if they were started; no-op otherwise."""
    global _cluster, _client
    if _client is not None:
        _client.close()
        _client = None
    if _cluster is not None:
        _cluster.close()
        _cluster = None
