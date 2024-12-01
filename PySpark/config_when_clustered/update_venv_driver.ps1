Remove-Item venv_driver -r # to remove the venv folder
py -3.10 -m venv venv_driver
.\venv_driver\Scripts\Activate.ps1
.\venv_driver\Scripts\python.exe -m pip install --upgrade pip
pip install -r .\requirements_driver.txt
# .\venv_ide\Scripts\Activate.ps1