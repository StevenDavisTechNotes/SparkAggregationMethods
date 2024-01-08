# Helpful Notes

## Installing Python on Windows
```
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
py -0  # to see what version is default
python --version # to double confirm

rm venv -r # to remove the venv folder
rm -r *.pyc
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1
python -c "import sys; print(sys.executable)"
.\venv\Scripts\python.exe -m pip install --upgrade pip
pip install -r .\requirements.txt

Then
Close and reopen VSCode

## Handy command lines
flake8 .
clear && flake8 . && pyright .
autopep8 --recursive --diff . | findstr /i /c:'--- original/'
autopep8 --recursive  --in-place .

## Installing Pyspark on Windows

- Python 3.11
  - https://www.python.org/downloads/
- Java JDK 17
  - https://www.oracle.com/java/technologies/downloads/#java17
  - Install to a folder without spaces in the same
    - `D:\Programs\Java\jdk-17`
- Spark
  - Download [https://spark.apache.org/downloads.html]
  - Save to `D:\Programs\spark-3.4.2-bin-hadoop3`
- Hadoop for Windows
  - Download [https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin]
  - Sve to `D:\Programs\hadoop-3.3.5\bin`
- Update requirements.txt
  - `pyspark[pandas_on_spark,sql]==3.4.2`
- Edit Environment Variables
  - HADOOP_HOME = D:\Programs\hadoop-3.3.5
  - JAVA_HOME = D:\Programs\Java\jdk-17
  - PYSPARK_DRIVER_PYTHON = python
  - PYSPARK_HADOOP_VERSION = 3
  - PYSPARK_PYTHON = python
  - SPARK_HOME = D:\Programs\spark-3.4.2-bin-hadoop3
 - Add to the PATH
   - %SPARK_HOME%\bin
   - %JAVA_HOME%\bin
   - %HADOOP_HOME%\bin
- Test
  - java --version
  - python --version
  - pyspark --version
  - pyspark
```

```py
words = "Hi there".split()
print(words)
rdd = sc.parallelize(words)
print(rdd.count())
print(" ".join(rdd.map(lambda x: x + "!").collect()))
```
