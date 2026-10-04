# Python Free-Threaded Aggregation Methods

> **Note**: This is a sub-project of the [Spark Aggregation Methods](../README.md) repository. Please refer to the parent repository for a full overview of the benchmarking project.

This directory explores how the aggregation challenges can be solved using Python multi-threading managed by queues.

## 🛠 Getting Started

The dependencies are managed by `uv` using the `pyproject.toml` file in this directory.

> **Note**: This directory contains a local `.python-version` file specifying `3.14t`. When you run `uv` commands in this folder, `uv` will automatically fetch and use the **free-threaded** (GIL-free) build of Python 3.14 to enable true multi-threading.

- From this directory, simply run `uv sync`.
- To execute a runner, use:

  ```bash
  uv run python -m src.perf_test_runner_for_py_ft
  ```
