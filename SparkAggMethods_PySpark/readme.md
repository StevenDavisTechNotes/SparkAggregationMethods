# Helpful Notes

<!-- cSpell: ignore venv, childitem, autopep8, pyclean, pyright, findstr, pycache, pytest, compat -->

## Installing Python on Windows
Use Microsoft Store or [download link](https://www.python.org/downloads/release/python-397/)
- Screen 1
    - Customize
- Optional Features (only check)
    - pip
    - py launcher
- Advanced Options (only check)
    - Precompile standard library
- Disable MAX_PATH

Go into Windows Terminal
```ps1
py -0  # to see what version is default
python --version # to double confirm

rm venv -r # to remove the venv folder
get-childitem src -include __pycache__ -recurse | remove-item -Force -Recurse
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
python -c "import sys; print(sys.executable)"
.\venv\Scripts\python.exe -m pip install --upgrade pip
pip install -r .\requirements.txt
pip3 freeze > frozen_requirements.txt
pip install -e 'C:\Src\GitHub_Hosted\SparkAggMethods2\SparkAggMethods_CommonPython\' --config-settings editable_mode=compat
```
Then Close and reopen VSCode

## Handy command lines

```
. .\venv\Scripts\Activate.ps1
flake8 src
.\venv\Scripts\Activate.ps1; clear && pyclean src && flake8 src && pyright src && python -m pytest src
autopep8 --recursive --diff src | findstr /i /c:'--- original/'
autopep8 --recursive  --in-place src
& "cspell-cli" "src/**/*.py" "--no-summary" "--no-progress" "--exclude" "__pycache__" "--exclude" ".git" "--exclude" "venv" "--fail-fast"
```

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


```py
words = "Hi there".split()
print(words)
rdd = sc.parallelize(words)
print(rdd.count())
print(" ".join(rdd.map(lambda x: x + "!").collect()))
```

## Debugging Dask
- `.compute(scheduler='single-threaded')`
