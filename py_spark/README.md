# PySpark Aggregation Methods

> **Note**: This is a sub-project of the [Spark Aggregation Methods](../README.md) repository. Please refer to the parent repository for a full overview of the benchmarking project.

## Helpful Notes

<!-- cSpell: ignore -->

### Installing PySpark on Windows

Look to see what version of Spark and Dask you are going to use, and then work backwards to what supported Python/JDK you can use.

- Spark
  - [Download](https://spark.apache.org/downloads.html)
    - Pre-built for Apache Hadoop 3.3 and later
    - Unzip to `C:\Programs\Spark\spark-3.5.9-bin-hadoop3`
- Python 3.10
  - https://www.python.org/downloads/
- Java JDK 17
  - https://learn.microsoft.com/en-us/java/openjdk/download#openjdk-17
    - Install for all users
    - Create JAVA_HOME environment variable
  - Old
    - https://www.oracle.com/java/technologies/downloads/#java17
    - Install to a folder without spaces in the name
      - `C:\Programs\Java\jdk-17`
- Hadoop for Windows
  - Download [winutils](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin)
  - Save to `C:\Programs\Spark\hadoop-3.3.6\bin`
- Edit Environment Variables for Your Account
  - HADOOP_HOME = C:\Programs\Spark\hadoop-3.3.6
  - PYSPARK_DRIVER_PYTHON = python
  - PYSPARK_HADOOP_VERSION = 3
  - PYSPARK_PYTHON = python
  - SPARK_HOME = C:\Programs\Spark\spark-3.5.9-bin-hadoop3
  - Add to the PATH (`%PATH%;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;%JAVA_HOME%\bin`)
    - %SPARK_HOME%\bin
    - %JAVA_HOME%\bin
    - %HADOOP_HOME%\bin
- Disable ShutdownHookManager logging
  - Due to a bug in Spark on Windows, the ShutdownHookManager will intermittently fail
  - `.\install_log4j2.ps1`
- Python Environment Setup
  - The Python dependencies are managed by `uv` at the repository root.
  - From the root directory, simply run `uv sync`.
  - To execute a runner, use `uv run python -m src.perf_test_runner_for_pyspark`.
- Test
  - java --version
  - python --version
  - pyspark --version
  - pyspark
