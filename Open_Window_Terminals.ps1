$folders = @(
    "SimpleQueuedPipelines",
    "CommonPython",
    "Admin",
    "PySpark",
    "PythonDask",
    "PythonOnly"
)
$cmd = "&{. .\venv\Scripts\Activate.ps1}"
foreach ($folder in $folders) {
    Start-Process wt -ArgumentList "-w 0 nt --title ${folder} -d ${folder} --suppressApplicationTitle powershell -NoExit -Command ${cmd}"
    Start-Sleep -Seconds 1
}