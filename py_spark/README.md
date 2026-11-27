# PySpark Aggregation Methods

> **Note**: This is a sub-project of the [Spark Aggregation Methods](../README.md) repository. Please refer to the parent repository for a full overview of the benchmarking project.

This directory explores how the aggregation challenges can be solved using PySpark.

## Helpful Notes

### Installing PySpark on Linux / WSL

The `pyspark` package installed by `uv sync` already bundles Spark, so the only hard prerequisite is a JDK 17. A separate Spark download is optional (needed only for the `pyspark` and `spark-submit` command-line tools).

- Java JDK 17
  - Debian/Ubuntu: `sudo apt update && sudo apt install -y openjdk-17-jdk-headless`
  - Other distributions: install the equivalent OpenJDK 17 package
- Spark (optional)
  - [Download](https://spark.apache.org/downloads.html) version 4.2.1, pre-built for Apache Hadoop 3.4 and later
  - Unpack to `~/spark/spark-4.2.1-bin-hadoop3`

```bash
mkdir -p ~/spark
tar -xzf spark-4.2.1-bin-hadoop3.tgz -C ~/spark
```

- Add to `~/.bashrc` (adjust `JAVA_HOME` to the output of `dirname $(dirname $(readlink -f $(which java)))`

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python
```

- If you separately installed Spark, then add to `~/.bashrc`:

```bash
export SPARK_HOME="$HOME/spark/spark-4.2.1-bin-hadoop3"
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"
```

- Python Environment Setup
  - From the repository root run `uv sync`, then `cd py_spark && uv sync`.
- Install Validation
  - `java --version` (should report 17)
  - `cd py_spark && uv run pyspark --version`
  - To execute a runner, use `uv run python -m src.perf_test_runner_for_pyspark`.
- WSL notes
  - Keep the repository and the test data on the Linux filesystem (`~/src/...`), not under `/mnt/c`; Spark I/O
    across the 9p mount is much slower and skews timings.
  - Results are logged under `results/<year>/wsl/`.

### Installing PySpark on Windows

Look to see what version of Spark and Dask you are going to use, and then work backwards to what supported Python/JDK you can use.

- Spark
  - [Download](https://spark.apache.org/downloads.html)
    - Version 4.2.1
    - Pre-built for Apache Hadoop 3.4 and later
    - Unzip to `C:\Programs\Spark\spark-4.2.1-bin-hadoop3`
- Java JDK 17
  - [Open JDK](https://learn.microsoft.com/en-us/java/openjdk/download#openjdk-17)
    - Install for all users
    - Create JAVA_HOME environment variable
  - [Oracle JDK](https://www.oracle.com/java/technologies/downloads/#java17)
    - Install to a folder without spaces in the name (`C:\Programs\Java\jdk-17`)
- Hadoop for Windows
  - [Instructions](https://kontext.tech/project/tools/article/hadoop-3-4-0-winutils-for-windows-10-x64)
    - Download [winutils](https://github.com/kontext-tech/winutils)

```ps1
git clone --depth 1 --filter=blob:none --sparse git@github.com:kontext-tech/winutils.git
cd winutils
git sparse-checkout set hadoop-3.4.0-win10-x64/bin
Copy-Item -Recurse -Force 'hadoop-3.4.0-win10-x64' 'C:\Programs\Spark\'
```

- Edit Environment Variables for Your Account
  - HADOOP_HOME=C:\Programs\Spark\hadoop-3.4.0-win10-x64
  - PYSPARK_DRIVER_PYTHON = python
  - PYSPARK_HADOOP_VERSION = 3
  - PYSPARK_PYTHON = python
  - SPARK_HOME = C:\Programs\Spark\spark-4.2.1-bin-hadoop3
  - Add to the PATH (`%PATH%;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;%JAVA_HOME%\bin`)
    - %SPARK_HOME%\bin
    - %JAVA_HOME%\bin
    - %HADOOP_HOME%\bin
- Python Environment Setup
  - The Python dependencies are managed by `uv` at the repository root.
  - From the root directory, simply run `uv sync`.
- Install Validation
  - java --version
  - python --version
  - pyspark --version
  - pyspark
  - To execute a runner, use `uv run python -m src.perf_test_runner_for_pyspark`.

### How the performance runner launches work

`src.perf_test_runner_for_pyspark` runs each challenge in child processes so that every child starts with a fresh JVM.
On Windows Spark starts a Python worker process per task and leaks one listening socket for each for the life of the
JVM. RDD strategies run the most tasks (measured: 130-270 leaked sockets per step, against 4-40 for DataFrame and SQL
strategies), and enough of them in one JVM exhaust the ephemeral port range (16,384 ports by default), failing tasks
with `SocketException: No buffer space available`. So on Windows each RDD step runs in a child process of its own and
a challenge's other steps share one child. On Linux and WSL Spark forks its workers from a daemon and does not leak,
so each challenge runs in a single child. RDD timings include JVM start-up in the per-step mode; they take minutes,
so it is small next to them.
