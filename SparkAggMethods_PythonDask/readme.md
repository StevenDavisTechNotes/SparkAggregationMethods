# Helpful Notes

<!-- cSpell: ignore venv, childitem, autopep8, pyclean, pyright, findstr, pycache, pytest, compat -->

## Installing Python on Windows
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
```ps1
py -0  # to see what version is default
python --version # to double confirm

rm venv -r # to remove the venv folder
get-childitem src -include __pycache__ -recurse | remove-item -Force -Recurse
py -3.12 -m venv venv
.\venv\Scripts\Activate.ps1
python -c "import sys; print(sys.executable)"
.\venv\Scripts\python.exe -m pip install --upgrade pip
pip install -r .\requirements.txt
pip3 freeze > frozen_requirements.txt
pip install -e 'C:\Src\GitHub_Hosted\SparkAggMethods2\SparkAggMethods_CommonPython\' --config-settings editable_mode=compat
```
Then Close and reopen VSCode

## Handy command lines

```
. .\venv\Scripts\Activate.ps1
flake8 src
.\venv\Scripts\Activate.ps1; clear && pyclean src && flake8 src && pyright src && python -m pytest src
autopep8 --recursive --diff src | findstr /i /c:'--- original/'
autopep8 --recursive  --in-place src
& "cspell-cli" "src/**/*.py" "--no-summary" "--no-progress" "--exclude" "__pycache__" "--exclude" ".git" "--exclude" "venv" "--fail-fast"
```

## Debugging Dask
- `.compute(scheduler='single-threaded')`
