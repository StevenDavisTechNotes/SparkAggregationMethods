# cSpell: ignore autopep8, childitem, findstr, isort, pycache, pyclean, pyright, pytest, venv

$DefaultScriptCommand = "Test"
$SrcFolderName = "spark_agg_methods_common_python"
$PythonVersion = "3.13"
$EditablePackageFolders = @()

. ..\makefile_python.ps1

if ($args.Length -eq 0) {
	& $DefaultScriptCommand
}
else {
	& $args[0]
}
