# usage: . .\run_all_runners.ps1

# cSpell: ignore venv

.\venv\Scripts\activate.ps1
python -O -m src.challenges.bi_level.bi_level_runner_dask --runs 0
python -O -m src.challenges.vanilla.vanilla_runner_dask --runs 0
