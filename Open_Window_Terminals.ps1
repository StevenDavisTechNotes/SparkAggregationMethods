$folders = @(
    "simple_queued_pipelines",
    "common_python",
    "admin",
    "py_spark",
    "python_dask",
    "python_free_threaded",
    "python_single_threaded"
)
$cmd = "&{. .\.venv\Scripts\activate.ps1}"
$cmd = "echo open"
foreach ($folder in $folders) {
    Start-Process wt -ArgumentList "-w 0 nt --title ${folder} -d ${folder} --suppressApplicationTitle powershell -NoExit -Command ${cmd}"
    Start-Sleep -Seconds 1
}
wt.exe -w 0 focus-tab -t 0


