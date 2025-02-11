# cSpell: ignore autopep8, childitem, findstr, isort, pycache, pyclean, pyright, pytest, venv

<#
.SYNOPSIS
This script automates various development tasks for a Python project.

.DESCRIPTION
The script provides functions to set up a virtual environment, build the package, 
search for spelling errors, run tests, and repair code formatting. 
It uses PowerShell to execute these tasks and ensures that the development 
environment is properly configured and maintained.

.EXAMPLE
.\makefile.ps1
Runs the default target, which is the Test function.
#>

function Build-Package() {
	<#
		.DESCRIPTION
		Builds the Python package using the build module.
	#>
	py -m build
}	

function Enable-Venv() {
	<#
		.DESCRIPTION
		Activates the virtual environment.
	#>
	.\venv\Scripts\Activate.ps1
}

function Install-Editable-Packages() {
	<#
		.DESCRIPTION
		Join editable packages to the virtual environment.
	#>
	$basePath = Split-Path (Get-Location).Path -Parent
	foreach ($packageDir in $EditablePackageFolders) {
		$packagePath = Join-Path $basePath $packageDir
		Write-Output "pip install --quiet --editable $packagePath --config-settings editable_mode=compat"
		pip install --quiet --editable $packagePath --config-settings editable_mode=compat
	}
}

function Install-Venv() {
	<#
		.DESCRIPTION
		Sets up a virtual environment, installs required packages, and saves the frozen requirements.
	#>
	if ($env:VIRTUAL_ENV) { deactivate }
	if (Test-Path .\venv) {
		Remove-Item .\venv -Recurse -Force
	}
	Remove-Item "*.egg-info" -Recurse -Force
	if (Test-Path ".pytest_cache") {
		Remove-Item ".pytest_cache" -Recurse -Force
	}
	py "-${PythonVersion}" -m venv venv
	Enable-Venv
	python --version
	python -c "import sys; print(sys.executable)"
	python -c "import sys; print('git_enabled', sys._is_gil_enabled() if hasattr(sys, '_is_gil_enabled') else None)"
	if ($?) { .\venv\Scripts\python.exe -m pip install --quiet --upgrade pip }
	if ($?) { Get-Content "requirements.txt" | Sort-Object | Set-Content "requirements.txt" }
	if ($?) { pip install  --quiet --requirement .\requirements.txt }
	if ($?) { Invoke-Pyclean }
	if ($?) { Write-Output "# cSpell: disable" > requirements_frozen.txt }
	if ($?) { pip freeze >> requirements_frozen.txt }
	if ($?) { Install-Editable-Packages }
}

function Invoke-Flake8() {
	<#
		.DESCRIPTION
		Runs flake8 to check the source folder for style guide enforcement.
	#>
	flake8 $SrcFolderName
}

function Invoke-ISort {
	param (
		[switch]$CheckOnly
	)
	<#
		.DESCRIPTION
		Runs isort to sort imports in the source folder. Can be run with the --check-only option.
	#>

	$arguments = @()
	if ($CheckOnly) {
		$arguments += '--check-only'
	}
	$arguments += $SrcFolderName

	& isort @arguments
}

function Invoke-Pyclean() {
	<#
		.DESCRIPTION
		Runs pyclean to remove auto-generated files.
	#>
	pyclean $SrcFolderName
}

function Invoke-Pyright() {
	<#
		.DESCRIPTION
		Runs pyright to get a thorough code analysis.
	#>
	pyright $SrcFolderName
}

function Repair-Format() {
	<#
		.DESCRIPTION
		Repairs the code formatting using autopep8 and isort.
	#>
	Enable-Venv
	if ($?) { Invoke-ISort -CheckOnly:$false }
	if ($?) { autopep8 --in-place --recursive $SrcFolderName }
}

function Search-Spelling() {
	<#
		.DESCRIPTION
		Run cspell-cli to identifying misspellings.
	#>
	& "cspell-cli" "${SrcFolderName}/**/*.py" `
		"--no-progress" "--fail-fast" `
		"--exclude" "__pycache__" `
		"--exclude" ".git" `
		"--exclude" "venv" 
}

function Install-Log4J2() {
	<#
		.DESCRIPTION
		Configured the Spark conf\log4j2.properties
	#>
	$src_path = Join-Path -Path $env:SPARK_HOME -ChildPath "conf\log4j2.properties.template"
	$dest_path = Join-Path -Path $env:SPARK_HOME -ChildPath "conf\log4j2.properties"
	$extra_path = 'log4j2_extra.properties'
	Copy-Item -Path $src_path -Destination $dest_path -Force 
	Get-Content -Path $extra_path | Add-Content -Path $dest_path
}

function Test() {
	<#
		.DESCRIPTION
		Run above functions to ensure that the code is in a correct state.
	#>
	Test-Without-UnitTests
	if ($?) { python -m pytest $SrcFolderName }
}

function Test-Without-UnitTests() {
	<#
		.DESCRIPTION
		Run static code analysis and spelling check without running unit tests.
	#>
	Clear-Host
	Enable-Venv
	if ($?) { Invoke-Pyclean }
	if ($?) { Invoke-Flake8 }
	if ($?) { Invoke-ISort -CheckOnly:$true }
	if ($?) { Invoke-Pyright }
	if ($?) { Search-Spelling }
}
