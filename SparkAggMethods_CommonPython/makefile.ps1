# cSpell: ignore autopep8, childitem, findstr, isort, pycache, pyclean, pyright, pytest, venv

$DefaultTarget = "Test"
$SrcFolderName = "spark_agg_methods_common_python"
$PythonVersion = "3.13"

function Build-Package() {
	py -m build
}	

function Enable-Venv() {
	.\venv\Scripts\Activate.ps1
}

function Install-Venv() {
	Remove-Item venv -r
	get-childitem $SrcFolderName -include __pycache__ -recurse | remove-item -Force -Recurse
	py "-${PythonVersion}" -m venv venv
	Enable-Venv
	python --version
	python -c "import sys; print(sys.executable)"
	.\venv\Scripts\python.exe -m pip install --upgrade pip
	pip install -r .\requirements.txt
	pip freeze > frozen_requirements.txt
}

function Repair-Format() {
	Enable-Venv
	autopep8 --in-place --recursive $SrcFolderName
}

function Search-Spelling() {
	& "cspell-cli" "${SrcFolderName}/**/*.py" `
		"--no-progress" "--fail-fast" `
		"--exclude" "__pycache__" `
		"--exclude" ".git" `
		"--exclude" "venv" 
}

function Test() {
	Test-Without-UnitTests
	if ($?) { python -m pytest $SrcFolderName }
}

function Test-Without-UnitTests() {
	Enable-Venv
	Clear-Host
	if ($?) { pyclean $SrcFolderName }
	if ($?) { flake8 $SrcFolderName }
	if ($?) { isort --check-only . $SrcFolderName }
	if ($?) { pyright $SrcFolderName }
	if ($?) { Search-Spelling }
}

if ($args.Length -eq 0) {
	& $DefaultTarget
}
else {
	& $args[0]
}
