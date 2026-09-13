# usage: . .\update_challenge_registry.ps1

.\.venv\Scripts\activate.ps1
python -O -m src.challenges.bi_level.bi_level_runner_dask --runs 0
python -O -m src.challenges.vanilla.vanilla_runner_dask --runs 0


