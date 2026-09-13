$src_path = Join-Path -Path $env:SPARK_HOME -ChildPath "conf\log4j2.properties.template"
$dest_path = Join-Path -Path $env:SPARK_HOME -ChildPath "conf\log4j2.properties"

Copy-Item -Path $src_path -Destination $dest_path -Force

# Disabled pending testing: these are log4j 1.x keys and are ignored by the
# log4j2 config this script writes. Re-enable in log4j2 syntax
# (logger.<id>.name / logger.<id>.level) only if the Windows
# ShutdownHookManager noise actually reappears on the current Spark version.
# @'
# # ignore known bug on Windows
# log4j.logger.org.apache.spark.util.ShutdownHookManager=OFF
# log4j.logger.org.apache.spark.SparkEnv=ERROR
# '@ | Add-Content -Path $dest_path

Write-Output "Copied log4j2 template to" $dest_path
