# Python Single-Threaded Aggregation Methods

> **Note**: This is a sub-project of the [Spark Aggregation Methods](../README.md) repository. Please refer to the parent repository for a full overview of the benchmarking project.

This directory contains the implementations for the aggregation challenges using single-threaded Python with and without GPU acceleration. These implementations serve as the performance and correctness baselines for the distributed frameworks.

## 🛠 Getting Started

The Python dependencies are managed by `uv` at the repository root.

- From the root directory, simply run `uv sync`.
- To execute a runner, use:

  ```bash
  uv run python -m src.perf_test_runner_for_py_st
  ```
