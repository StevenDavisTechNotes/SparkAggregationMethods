# Helpful Notes

<!-- cSpell: ignore venv, childitem, autopep8, pyclean, pyright, findstr, pycache, pytest -->

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
get-childitem spark_agg_methods_common_python -include __pycache__ -recurse | remove-item -Force -Recurse
py -3.13 -m venv venv
.\venv\Scripts\Activate.ps1
python -c "import sys; print(sys.executable)"
.\venv\Scripts\python.exe -m pip install --upgrade pip
pip install -r .\requirements.txt
pip freeze > frozen_requirements.txt
py -m build
```
Then Close and reopen VSCode

## Handy command lines

```
. .\venv\Scripts\Activate.ps1
flake8 spark_agg_methods_common_python
.\venv\Scripts\Activate.ps1 ; clear ; if ($?) { pyclean spark_agg_methods_common_python } ; if ($?) { flake8 spark_agg_methods_common_python } ; if ($?) { pyright spark_agg_methods_common_python } ; if ($?) { python -m pytest spark_agg_methods_common_python }
autopep8 --recursive --diff spark_agg_methods_common_python | findstr /i /c:'--- original/'
autopep8 --recursive  --in-place spark_agg_methods_common_python
& "cspell-cli" "spark_agg_methods_common_python/**/*.py" "--no-summary" "--no-progress" "--exclude" "__pycache__" "--exclude" ".git" "--exclude" "venv" "--fail-fast"
```
