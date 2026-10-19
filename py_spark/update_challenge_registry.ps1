# usage: . .\update_challenge_registry.ps1
# Leaves the names of any failed runner modules in $registryFailures (empty on success).

$project = Split-Path -Leaf $PSScriptRoot
$modules = @(
    'src.challenges.bi_level.bi_level_runner_pyspark'
    'src.challenges.conditional.conditional_runner_pyspark'
    'src.challenges.deduplication.dedupe_runner_pyspark'
    'src.challenges.sectional.section_runner_pyspark'
    'src.challenges.vanilla.vanilla_runner_pyspark'
)

. .\.venv\Scripts\activate.ps1
$registryFailures = @()
foreach ($module in $modules) {
    Write-Output "[$project] python -m $module --runs 0"
    python -m $module --runs 0
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "[$project] FAILED (exit code $LASTEXITCODE): $module"
        $registryFailures += $module
    }
}
if ($registryFailures.Count -eq 0) {
    Write-Output "[$project] OK"
} else {
    Write-Warning "[$project] $($registryFailures.Count) of $($modules.Count) runners failed"
}
