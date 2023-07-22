# Installing Python on Windows
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
py -3.10 -m venv venv
.\venv\Scripts\Activate.ps1
.\venv\Scripts\python.exe -m pip install --upgrade pip
pip install -r .\requirements.txt

Then
Close and reopen VSCode

# Handy command lines
flake8 .
autopep8 --recursive --diff . | findstr /i /c:'--- original/'
autopep8 --recursive  --in-place .


