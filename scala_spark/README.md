# Scala Spark Aggregation Methods

> **Note**: This is a sub-project of the [Spark Aggregation Methods](../README.md) repository. Please refer to the parent repository for a full overview of the benchmarking project.

This directory contains the Scala implementations for the Spark aggregation challenges. It demonstrates how to use the native Scala API (RDDs, DataFrames, Datasets, and UDAFs) to solve complex aggregation problems and benchmarks their performance against the Python equivalents.

## 🛠 Getting Started

We use `sbt` as the build tool for the Scala projects.

### Prerequisites
- Java JDK 17 (see the JDK step in [`../py_spark/README.md`](../py_spark/README.md); the Scala build needs only
  the JDK, not a separate Spark download, because `build.sbt` pulls Spark 3.5.9 as a dependency)
- `sbt` (Scala Build Tool), installed as below

The versions are pinned by the repository, so any recent `sbt` launcher works: `project/build.properties` selects
sbt 1.9.9 and `build.sbt` selects Scala 2.12.18. The launcher downloads those on first run, so the first
`sbt compile` needs network access and takes a few minutes.

### Installing sbt

**Linux / WSL (Debian/Ubuntu), apt repository**
```bash
sudo apt-get update
sudo apt-get install -y apt-transport-https curl gnupg
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
  | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
sudo apt-get update
sudo apt-get install -y sbt
```

**Linux / WSL, without root (SDKMAN)**
```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install sbt
```

**Windows**
```powershell
winget install sbt.sbt
```
or download the installer from <https://www.scala-sbt.org/download/>. Open a new terminal afterwards so `PATH` is refreshed.

**Check the install**
```bash
java --version        # 17
sbt --version         # prints the sbt launcher version and, inside scala_spark, sbt 1.9.9
cd scala_spark && sbt compile
```

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
   sbt "runMain challenges.dedupe.IDERunner"
   ```
