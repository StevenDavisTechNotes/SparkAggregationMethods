# usage: . .\quick_test_run.ps1

..\.venv\Scripts\activate.ps1
python -m src.challenges.bi_level.bi_level_runner_pyspark --runs 1 --no-shuffle --size 3_3_10 3_30_10k
python -m src.challenges.conditional.conditional_runner_pyspark --runs 1 --no-shuffle --size 3_3_1 3_3_10 3_3_100
python -m src.challenges.deduplication.dedupe_runner_pyspark --runs 1 --no-shuffle --size 2 3 6 20 30 60
python -m src.challenges.sectional.section_runner_pyspark --runs 1 --no-shuffle --size 1 10 100
python -m src.challenges.vanilla.vanilla_runner_pyspark --runs 1 --no-shuffle --size 3_3_1 3_3_10 3_3_100

