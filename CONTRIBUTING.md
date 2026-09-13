# Contributing to Spark Aggregation Methods

First off, thank you for considering contributing to Spark Aggregation Methods! This project is intended to be a comprehensive benchmark and sandbox for complex data aggregation patterns.

## Development Environment Setup

### Python Environments
See [Getting Started](README.md#-getting-started) in the README for
environment setup. Each sub-project is a separate `uv` project and is synced
individually; there is no single root `uv sync` that installs everything.

### Scala Environment
We use `sbt` for building the `scala_spark` component.
1. Navigate to the `scala_spark` directory.
2. Run `sbt compile` or `sbt test` to build the Scala implementations.

## Code Standards

`pre-commit` enforces `isort`, `autopep8`, `flake8`, and `ty`. See
[Code Standards](README.md#5-code-standards-pre-commit) in the README for
installation, including the submodule's own hooks.

## Submitting Pull Requests
1. Ensure you have run the tests and benchmarks for the module you modified.
2. Ensure your code passes all `pre-commit` checks.
3. Open a Pull Request with a clear description of the aggregation logic or optimization you've introduced!
