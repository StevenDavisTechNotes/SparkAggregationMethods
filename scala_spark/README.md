# Scala Spark Aggregation Methods

> **Note**: This is a sub-project of the [Spark Aggregation Methods](../README.md) repository. Please refer to the parent repository for a full overview of the benchmarking project.

This directory contains the Scala implementations for the Spark aggregation challenges. It demonstrates how to use the native Scala API (RDDs, DataFrames, Datasets, and UDAFs) to solve complex aggregation problems and benchmarks their performance against the Python equivalents.

## 🛠 Getting Started

We use `sbt` as the build tool for the Scala projects.

### Prerequisites
- Java JDK 17
- `sbt` (Scala Build Tool)

### Building and Running
1. Navigate to this directory:
   ```bash
   cd scala_spark
   ```
2. Compile the project:
   ```bash
   sbt compile
   ```
3. Run the automated tests:
   ```bash
   sbt test
   ```
   There are currently no Scala test sources, so this is a placeholder that
   passes vacuously.
4. Run a challenge (each has its own entry point, so pick one):
   ```bash
   sbt "runMain challenges.vanilla.Runner"
   sbt "runMain challenges.deduplication.IDERunner"
   ```
