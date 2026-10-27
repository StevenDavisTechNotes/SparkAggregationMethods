# usage: . .\quick_test_run.ps1

.\.venv\Scripts\activate.ps1
$failed = $false
python -m src.challenges.bi_level.bi_level_runner_dask --runs 1 --no-shuffle --size 3_3_10 3_30_10k
if ($LASTEXITCODE -ne 0) { $failed = $true }
python -m src.challenges.vanilla.vanilla_runner_dask --runs 1 --no-shuffle --size 3_3_1 3_3_10 3_3_100
if ($LASTEXITCODE -ne 0) { $failed = $true }

# Exit non-zero if any runner failed, so callers such as scripts/run_all.py --quick-run can see it.
# When dot-sourced (. .\quick_test_run.ps1) `exit` would close the caller's shell, so only report there.
if ($failed) {
    if ($MyInvocation.InvocationName -eq '.') {
        Write-Error 'One or more runners failed.'
    } else {
        exit 1
    }
}
