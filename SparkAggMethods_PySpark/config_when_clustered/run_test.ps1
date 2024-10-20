$env:PYSPARK_PYTHON = "$pwd\venv_executor\scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$pwd\venv_driver\scripts\python.exe"

spark-submit --master local[8] --driver-memory 1g --deploy-mode client `
--conf spark.pyspark.python=$env:PYSPARK_PYTHON `
--conf spark.pyspark.driver.python=$env:PYSPARK_DRIVER_PYTHON `
--archives '..\bin\spark\venv_executor.tar.gz#venv_executor' `
'test.py'