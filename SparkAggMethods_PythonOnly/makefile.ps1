# cSpell: ignore autopep8, childitem, findstr, isort, pycache, pyclean, pyright, pytest, venv

$DefaultTarget = "Test"
$SrcFolderName = "src"
$PythonVersion = "3.13"
$EditablePackageNames = @(
	"SimpleQueuedPipelines", 
	"CommonPython"
)

function Enable-Venv() {
	.\venv\Scripts\Activate.ps1
}

function Install-Editable-Package($packageDir = "CommonPython") {
    $basePath = (Get-Location).Path
    $parentPath = Split-Path $basePath -Parent
    $commonPythonPath = Join-Path $parentPath $packageDir
    pip install -e $commonPythonPath --config-settings editable_mode=compat
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
	foreach ($packageName in $EditablePackageNames) {
		Install-Editable-Package $packageName
	}
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
