$DefaultScriptCommand = "Test"
$SrcFolderName = "src"
$PythonVersion = "3.13t"
$EditablePackageFolders = @(
	"SimpleQueuedPipelines", 
	"CommonPython"
)

. ..\makefile_python.ps1

if ($args.Length -eq 0) {
	& $DefaultScriptCommand
}
else {
	& $args[0]
}
