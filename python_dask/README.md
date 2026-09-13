# Python Dask Aggregation Methods

> **Note**: This is a sub-project of the [Spark Aggregation Methods](../README.md) repository. Please refer to the parent repository for a full overview of the benchmarking project.

## Helpful Notes

### Debugging Dask
- `.compute(scheduler='single-threaded')`

## 🛠 Getting Started
The dependencies are managed by `uv` using the `pyproject.toml` file in this directory.

> **Note**: This directory contains a local `.python-version` file specifying `3.12`. When you run `uv` commands in this folder, `uv` will automatically fetch and use Python 3.12 to maintain compatibility with `dask-sql`.

- From this directory, simply run `uv sync`.
- To execute a runner, use:
  ```bash
  uv run python -m src.perf_test_runner_for_dask
  ```
