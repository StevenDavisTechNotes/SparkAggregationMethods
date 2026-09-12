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

* **`PySpark`**: Apache Spark implementations using the Python API (RDDs, DataFrames, Pandas UDFs).
* **`ScalaSpark`**: Apache Spark implementations using the native Scala API (RDDs, Datasets/DataFrames, UDAFs).
* **`PythonDask`**: Parallel computing using the Dask framework (Dask DataFrames, Dask Bags).
* **`PythonSingleThreaded`**: Baseline implementations using standard single-threaded Python (Pandas, Numpy) for comparison against distributed frameworks.
* **`PythonStreaming`**: Python implementations utilizing multi-threading and queues for stream processing.

*Note: The `CommonPython` folder contains shared utilities, types, and test harness code used across the Python-based engines.*

## 🛠 Getting Started

Each engine has its own specific dependencies and setup instructions. Navigate to the specific sub-project folder for detailed instructions:

1. **Simple Queued Pipelines**:
   * Run `git submodule add git@github.com:StevenDavisTechNotes/simple_queued_pipelines.git SimpleQueuedPipelines`

1. **Python Environments**: 
   * Navigate to the target directory (e.g., `cd PySpark`).
   * Review the local `readme.md` for specific requirements (e.g., Spark, Hadoop, JDK versions).
   * Use the included `makefile.ps1` or run the setup scripts to initialize the virtual environment and install dependencies from `requirements.txt`.
   * Run the performance tests using the respective runner scripts (e.g., `python -m src.perf_test_runner_for_pyspark`).

2. **Scala Environment**:
   * Navigate to `cd ScalaSpark`.
   * Open the project in IntelliJ IDEA or compile/run using your preferred Scala build tool (e.g., sbt or Maven, depending on local configuration).

## 📊 Results

When you execute the performance test runners (e.g., `perf_test_runner_for_dask.py`), the system generates CSV output files in the `results/` directories. These log files contain execution times, partition counts, data sizes, and correctness verification results, which are used to generate the graphs and performance comparisons seen in the reference articles.

## 📝 License

Please refer to the `LICENSE` file in the root directory for usage and distribution terms.
