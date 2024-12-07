$folders = @(
    "SimpleQueuedPipelines",
    "CommonPython",
    "Admin",
    "PySpark",
    "PythonDask",
    "PythonSingleThreaded",
    "PythonStreaming"
)
$cmd = "&{. .\venv\Scripts\Activate.ps1}"
$cmd = "echo open"
foreach ($folder in $folders) {
    Start-Process wt -ArgumentList "-w 0 nt --title ${folder} -d ${folder} --suppressApplicationTitle powershell -NoExit -Command ${cmd}"
    Start-Sleep -Seconds 1
}
wt.exe -w 0 focus-tab -t 0
