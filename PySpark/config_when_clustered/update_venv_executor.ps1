Remove-Item venv_executor -r # to remove the venv folder
py -3.10 -m venv venv_executor
.\venv_executor\Scripts\Activate.ps1
.\venv_executor\Scripts\python.exe -m pip install --upgrade pip
pip install -r .\executor_requirements.txt
.\venv\Scripts\Activate.ps1
mkdir ..\bin\python
if (Test-Path '..\bin\python\venv_executor.zip') {
    Remove-Item '..\bin\python\venv_executor.zip'
}
Set-Location venv_executor
Compress-Archive -Path '.\*' -DestinationPath '..\..\bin\python\venv_executor.zip'
