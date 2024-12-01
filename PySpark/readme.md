# Helpful Notes

<!-- cSpell: ignore -->

## Installing PySpark on Windows

Look to see what version of Spark and Dask you are going to use, and then work backwards to what supported Python/JDK you can use.

- Spark
  - (Download)(https://spark.apache.org/downloads.html)
    - Pre-built for Apache Hadoop 3.3 and later
    - Unzip to `D:\Programs\spark-3.4.3-bin-hadoop3`
- Python 3.11
  - https://www.python.org/downloads/
- Java JDK 17
  - https://www.oracle.com/java/technologies/downloads/#java17
  - Install to a folder without spaces in the same
    - `D:\Programs\Java\jdk-17`
- Hadoop for Windows
  - Download [https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin]
  - Sve to `D:\Programs\hadoop-3.3.5\bin`
- Update requirements.txt
  - `pyspark[pandas_on_spark,sql]==3.4.3`
- Edit Environment Variables for Your Account
  - HADOOP_HOME = D:\Programs\hadoop-3.3.5
  - JAVA_HOME = D:\Programs\Java\jdk-17
  - PYSPARK_DRIVER_PYTHON = python
  - PYSPARK_HADOOP_VERSION = 3
  - PYSPARK_PYTHON = python
  - SPARK_HOME = D:\Programs\spark-3.4.3-bin-hadoop3
 - Add to the PATH
   - %SPARK_HOME%\bin
   - %JAVA_HOME%\bin
   - %HADOOP_HOME%\bin
- Test
  - java --version
  - python --version
  - pyspark --version
  - pyspark
