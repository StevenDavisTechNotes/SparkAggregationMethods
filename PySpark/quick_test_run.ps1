# usage: . .\run_all_runners.ps1

# cSpell: ignore venv

.\venv\Scripts\activate.ps1
python -O -m src.challenges.bi_level.bi_level_runner_pyspark --runs 1 --no-shuffle --size 3_3_10 3_30_10k
python -O -m src.challenges.conditional.conditional_runner_pyspark --runs 1 --no-shuffle --size 3_3_1 3_3_10 3_3_100
python -O -m src.challenges.deduplication.dedupe_runner_pyspark --runs 1 --no-shuffle --size 2 3 6 20 30 60
python -O -m src.challenges.sectional.section_runner_pyspark --runs 1 --no-shuffle --size 1 10 100
python -O -m src.challenges.vanilla.vanilla_pyspark_runner_pyspark --runs 1 --no-shuffle --size 3_3_1 3_3_10 3_3_100
