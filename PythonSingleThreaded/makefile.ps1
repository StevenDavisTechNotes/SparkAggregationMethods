# cSpell: ignore autopep8, childitem, findstr, isort, pycache, pyclean, pyright, pytest, venv

$DefaultScriptCommand = "Test"
$SrcFolderName = "src"
$PythonVersion = "3.13"
$EditablePackageFolders = @(
	"CommonPython"
)

. ..\makefile_python.ps1

if ($args.Length -eq 0) {
	& $DefaultScriptCommand
}
else {
	& $args[0]
}
