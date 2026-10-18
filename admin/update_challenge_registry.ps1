# usage: . .\update_challenge_registry.ps1
# Runs each sub-project's update_challenge_registry.ps1 and reports which projects/runners failed.

$repoRoot = Split-Path -Parent $PSScriptRoot
$projects = @('py_spark', 'python_dask', 'python_free_threaded', 'python_single_threaded')
$failedProjects = [ordered]@{}

foreach ($project in $projects) {
    Write-Output "=== $project ==="
    Push-Location (Join-Path $repoRoot $project)
    try {
        $registryFailures = @()
        . .\update_challenge_registry.ps1
        if ($registryFailures.Count -gt 0) {
            $failedProjects[$project] = $registryFailures
        }
    } catch {
        Write-Warning "[$project] script error: $($_.Exception.Message)"
        $failedProjects[$project] = @("script error: $($_.Exception.Message)")
    } finally {
        Pop-Location
    }
}

if (Get-Command deactivate -ErrorAction SilentlyContinue) {
    deactivate
}

if ($failedProjects.Count -eq 0) {
    Write-Output "Success"
} else {
    Write-Warning "FAILED in $($failedProjects.Count) of $($projects.Count) projects:"
    foreach ($project in $failedProjects.Keys) {
        Write-Warning "  $project"
        foreach ($failure in $failedProjects[$project]) {
            Write-Warning "    $failure"
        }
    }
}
