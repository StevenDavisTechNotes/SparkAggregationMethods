# Spark Aggregation Methods

Welcome to the **Spark Aggregation Methods** repository. This project serves as a comprehensive benchmark, sandbox, and reference implementation for complex data aggregation patterns. It explores how to perform various types of aggregations across multiple data processing frameworks, comparing their ease of implementation, syntax, and performance.

This codebase acts as the companion code and continuation of the methodologies explored in [Steven Davis's Tech Notes](https://stevendavistechnotes.blogspot.com/) regarding Apache Spark aggregations.

## 🎯 The Challenges

The project implements solutions for five distinct aggregation patterns. Each challenge represents a common real-world data processing scenario:

1. **Vanilla Aggregation**: Standard GroupBy and reduction operations.
   * *Reference*: [Apache Spark Vanilla Aggregate Example](https://stevendavistechnotes.blogspot.com/2018/06/apache-spark-vanilla-aggregate-example.html)
2. **Conditional Aggregation**: Aggregating fields conditionally based on the values of other fields within the same group.
   * *Reference*: [Apache Spark Conditional Aggregation](https://stevendavistechnotes.blogspot.com/2018/06/apache-spark-conditional-aggregation.html)
3. **Bi-Level Aggregation**: Performing aggregations at a sub-group level and then aggregating those results up to a parent-group level.
   * *Reference*: [Apache Spark Bi-Level Aggregation](https://stevendavistechnotes.blogspot.com/2018/06/apache-spark-bi-level-aggregation.html)
4. **Sectional (Sequential) Processing**: Processing ordered data where calculations depend on the previous row's state (e.g., sessionization or cumulative calculations).
   * *Reference*: [Apache Spark Sequential Processing](https://stevendavistechnotes.blogspot.com/2018/07/apache-spark-sequential-processing.html)
5. **Deduplication**: Efficiently removing duplicate records based on complex matching criteria or prioritizing certain records.
   * *Reference*: [Apache Spark Deduplication](https://stevendavistechnotes.blogspot.com/2018/08/apache-spark-dedupe-deduplication.html)

## 🚀 Processing Engines (Frameworks)

To compare syntax and performance, the challenges are implemented across several different computation engines, separated into their respective top-level directories:

* **`py_spark`**: Apache Spark implementations using the Python API (RDDs, DataFrames, Pandas UDFs).
* **`scala_spark`**: Apache Spark implementations using the native Scala API (RDDs, Datasets/DataFrames, UDAFs).
* **`python_dask`**: Parallel computing using the Dask framework (Dask DataFrames, Dask Bags).
* **`python_single_threaded`**: Baseline implementations using standard single-threaded Python (Pandas, Numpy) for comparison against distributed frameworks.
* **`python_free_threaded`**: Python implementations utilizing multi-threading and queues for stream processing.

*Note: The `common_python` folder contains shared utilities, types, and test harness code used across the Python-based engines.*

## 🛠 Getting Started

We use **`uv`** for Python dependency management and **`sbt`** for compiling Scala projects.

### 1. Prerequisites

* **Python environments**: Install `uv`.
* **Scala environment**: Install `sbt` and Java JDK 17.

### 2. Code Architecture

#### Git Repos

```bash
git clone git@github.com:StevenDavisTechNotes/SparkAggregationMethods.git .
git clone git@github.com:StevenDavisTechNotes/simple_queued_pipelines.git simple_queued_pipelines
git submodule update --init --recursive
```

#### Python VENV

| Path | Version | Description | Version Limitation |
| ------ | --------- | --------- | --------- |
| . | 3.10 | Pre-commit | LCD |
| admin | 3.14 | Pre-and-post test processing | None |
| py_spark | 3.10 | PySpark implementations w/wo GPU | Spark 3.5.9 |
| python_dask | 3.12 | Dask implementations | `dask-sql` |
| python_free_threaded | 3.14t | Multi-threaded implementations | GIL removed; PyO3 (Rust) wheels require >= 3.14t |
| python_single_threaded | 3.14 | numpy w/wo GPU | Match python_free_threaded w GIL |
| simple_queued_pipelines | 3.14t | Reusable components used by python_free_threaded | GIL removed; PyO3 (Rust) wheels require >= 3.14t |

#### Python Projects

| Path | PYTHONROOT | Description |
| ------ | --------- | --------- |
| . | N/A | Not a project |
| admin | src | Pre-and-post test processing |
| common_python | spark_agg_methods_common_python | types, utilities, test harness framework |
| py_spark | 3.10 | PySpark implementations w/wo GPU |
| python_dask | src | Dask implementations |
| python_free_threaded | src | Multi-threaded implementations |
| python_single_threaded | src | numpy w/wo GPU |
| simple_queued_pipelines | simple_queued_pipelines | Reusable components used by python_free_threaded |

#### VSCode Usage

Primary entrypoint is `python-workspace.code-workspace`.

### 3. Buliding Python Environments (`uv`)

Because this repository tests against radically different Python capabilities (legacy Spark vs modern free-threading), the projects cannot share a single workspace lockfile. You must sync the environments for the specific projects you wish to run:

```bash
(
   uv sync && 
   cd admin && uv sync && cd .. && 
   cd py_spark && uv sync && cd .. && 
   cd python_dask && uv sync && cd .. && 
   cd python_single_threaded && uv sync && cd .. && 
   cd simple_queued_pipelines && uv sync && cd .. &&
   cd python_free_threaded && uv sync && cd .. && 
   echo "Environments updated"
)

uv run pyclean --debrise admin challenge_base common_python py_spark python_dask python_free_threaded python_single_threaded simple_queued_pipelines 

```

This automatically handles the different Python requirements across our projects:

* `py_spark` targets **Python 3.10** for native compatibility with Spark 3.5.9.
* `python_dask` targets **Python 3.12** for compatibility with the `dask-sql` ecosystem.
* `admin` and `python_single_threaded` target standard **Python 3.14**.
* `python_free_threaded` and `simple_queued_pipelines` target **Python 3.14t (free-threaded)** to leverage true multi-threading without the GIL. (This is enforced via local `.python-version` files).

### 4. Building Scala Environment (`sbt`)

* Install sbt from `https://www.scala-sbt.org/download/`

* The `scala_spark` directory contains our native Scala implementations.

```bash
cd scala_spark
sbt compile
sbt test  # no Scala test sources yet, so this passes vacuously
sbt "runMain challenges.vanilla.Runner"
sbt "runMain challenges.deduplication.IDERunner"
```

### 5. Code Standards (`pre-commit`)

We use `pre-commit` to automatically run formatting (`isort`, `autopep8`) and linting (`flake8`, `ty`) before you commit code.
To install the pre-commit hooks into your local git repository, run:

```bash
uv run pre-commit install && cd simple_queued_pipelines && uv run pre-commit install && cd ..
uv run pre-commit run --all-files
uv run pre-commit run --hook-type pre-push --all-files

```

To manually run all test suites across every sub-project at once, you can use the master test runner:

```bash
uv run python scripts/run_all.py --pytest
```

To run a specific performance test in a sub-project, use `uv run` locally:

```bash
cd py_spark
uv run python -m src.perf_test_runner_for_pyspark
```

## 📊 Results

When you execute the performance test runners (e.g., `perf_test_runner_for_dask.py`), the system generates CSV output files in the `results/` directories. These log files contain execution times, partition counts, data sizes, and correctness verification results, which are used to generate the graphs and performance comparisons seen in the reference articles.

## 📝 License

Please refer to the `LICENSE` file in the root directory for usage and distribution terms.
