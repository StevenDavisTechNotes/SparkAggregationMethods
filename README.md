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
| . | 3.14 | Pre-commit; packages `common_python` for the other projects | None |
| admin | 3.14 | Pre-and-post test processing | None |
| common_python | 3.14 | Shared types, utilities, test harness (own `.venv`) | LCD |
| py_spark | 3.14 | PySpark implementations w/wo GPU | Spark 4.2.0 |
| python_dask | 3.14 | Dask implementations | |
| python_free_threaded | 3.14t | Multi-threaded implementations | GIL removed; PyO3 (Rust) wheels require >= 3.14t |
| python_single_threaded | 3.14 | numpy w/wo GPU | Match python_free_threaded w GIL |
| simple_queued_pipelines | 3.14t | Reusable components used by python_free_threaded | GIL removed; PyO3 (Rust) wheels require >= 3.14t |

#### Python Projects

| Path | PYTHONROOT | Description |
| ------ | --------- | --------- |
| . | N/A | Not a project |
| admin | src | Pre-and-post test processing |
| common_python | spark_agg_methods_common_python | types, utilities, test harness framework |
| py_spark | src | PySpark implementations w/wo GPU |
| python_dask | src | Dask implementations |
| python_free_threaded | src | Multi-threaded implementations |
| python_single_threaded | src | numpy w/wo GPU |
| simple_queued_pipelines | simple_queued_pipelines | Reusable components used by python_free_threaded |

#### VSCode Usage

Primary entrypoint is `python-workspace.code-workspace`.

### 3. Building Python Environments (`uv`)

Because this repository tests against radically different Python capabilities (legacy Spark vs modern free-threading), the projects cannot share a single workspace lockfile. You must sync the environments for the specific projects you wish to run:

```bash
(
   uv sync && uv pip install --upgrade pip &&
   cd common_python && uv sync && uv pip install --upgrade pip && cd .. && 
   cd simple_queued_pipelines && uv sync && uv pip install --upgrade pip && cd .. &&
   cd admin && uv sync && uv pip install --upgrade pip && cd .. && 
   cd py_spark && uv sync && uv pip install --upgrade pip && cd .. && 
   cd python_dask && uv sync && uv pip install --upgrade pip && cd .. && 
   cd python_single_threaded && uv sync && uv pip install --upgrade pip && cd .. && 
   cd python_free_threaded && uv sync && uv pip install --upgrade pip && cd .. && 
   echo "Environments updated"
)

uv run pyclean admin common_python py_spark python_dask python_free_threaded python_single_threaded simple_queued_pipelines --debris

```

```ps1
                                  ( uv sync ); ( uv pip install --upgrade pip );
  ( cd common_python           ); ( uv sync ); ( uv pip install --upgrade pip ); ( cd .. );
  ( cd simple_queued_pipelines ); ( uv sync ); ( uv pip install --upgrade pip ); ( cd .. );
  ( cd admin                   ); ( uv sync ); ( uv pip install --upgrade pip ); ( cd .. );
  ( cd py_spark                ); ( uv sync ); ( uv pip install --upgrade pip ); ( cd .. );
  ( cd python_dask             ); ( uv sync ); ( uv pip install --upgrade pip ); ( cd .. );
  ( cd python_single_threaded  ); ( uv sync ); ( uv pip install --upgrade pip ); ( cd .. );
  ( cd python_free_threaded    ); ( uv sync ); ( uv pip install --upgrade pip ); ( cd .. );

uv run pyclean admin common_python py_spark python_dask python_free_threaded python_single_threaded simple_queued_pipelines --debris

```

This automatically handles the different Python requirements across our projects:

* `.` (root), `common_python`, `py_spark`, `admin`, `python_dask` and `python_single_threaded` target standard **Python 3.14**. `py_spark` needs it to match Spark 4.2.0.
* `common_python`'s `pyproject.toml` is derived from the root one, so a dependency change to the shared package must be made in both files.
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
uv run pre-commit run --hook-stage pre-push --all-files

```

To run the code-quality tools across every Python sub-project at once, use `scripts/run_all.py`. It runs each selected tool in turn over every sub-project (`admin`, `common_python`, `py_spark`, `python_dask`, `python_free_threaded`, `python_single_threaded`, `simple_queued_pipelines`), each through that project's own `uv` environment, and prints which folders failed.

```bash
uv run python scripts/run_all.py [--autopep8] [--flake8] [--ty] [--pytest] [--pyright] [--exclude PROJECT ...]
```

| Option | Description |
| ------ | --------- |
| `--autopep8` | Run `autopep8 --in-place --recursive` (rewrites files; line length read from `setup.cfg`). |
| `--flake8` | Run `flake8` (line length read from `setup.cfg`). |
| `--ty` | Run `ty check`, with extra search paths derived from each project's `[tool.uv.sources]`. |
| `--pytest` | Run `pytest`. Exit code 5 (no tests collected) is treated as success, since several sub-projects have no tests yet. |
| `--pyright` | Run the globally installed (npm) `pyright` against each project's own `.venv`. Opt-in only: it is never part of the default set. |
| `--exclude PROJECT` | Skip a sub-project for every selected tool. Repeatable. `PROJECT` must be one of the sub-project folders listed above. |
| `-h`, `--help` | Show usage and exit. |

Tool selection:

* With no tool flag, `autopep8`, `flake8`, `ty` and `pytest` run (in that order). `pyright` is not included.
* With one or more tool flags, only those tools run. Tools always run in the order `autopep8`, `flake8`, `ty`, `pytest`, `pyright`, regardless of flag order.
* The exit code is `0` if every tool passes in every non-excluded project, otherwise `1`. A failure in one project does not stop the remaining projects or tools; failures are summarized at the end.

Pyright requires a global npm install (`npm install -g pyright`).

```bash
# Default set: autopep8, flake8, ty, pytest
uv run python scripts/run_all.py

# Only the test suites
uv run python scripts/run_all.py --pytest

# Lint and type check, skipping two sub-projects
uv run python scripts/run_all.py --flake8 --ty --exclude py_spark --exclude python_dask

# Opt-in pyright check
uv run python scripts/run_all.py --pyright

# Update the challenge registry JSON from code
cd admin; . .\update_challenge_registry.ps1

# Generate the data files used by the tests
cd admin; uv run python -m src.generate_data_files
cd py_spark; uv run python -m src.generate_data_files_pyspark

# Smoke test the platform by running all of the strategies across the smallest datasets
uv run python scripts/run_all.py --quick-run

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
