Remove-Item venv_ide -r # to remove the venv folder
py -3.10 -m venv venv_ide
.\venv_ide\Scripts\Activate.ps1
.\venv_ide\Scripts\python.exe -m pip install --upgrade pip
pip install -r .\requirements_ide.txt
