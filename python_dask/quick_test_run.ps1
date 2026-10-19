# usage: . .\run_all_runners.ps1

.\.venv\Scripts\activate.ps1
python -m src.challenges.bi_level.bi_level_runner_dask --runs 1 --no-shuffle --size 3_3_10 3_30_10k
python -m src.challenges.vanilla.vanilla_runner_dask --runs 1 --no-shuffle --size 3_3_1 3_3_10 3_3_100


