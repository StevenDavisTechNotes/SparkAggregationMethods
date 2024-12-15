# usage: . .\run_all_runners.ps1
# cSpell: ignore venv

.\venv\Scripts\activate.ps1
# python -O -m src.challenges.bi_level.bi_level_runner_py_stream --runs 0
# python -O -m src.challenges.conditional.conditional_runner_py_stream --runs 0
# python -O -m src.challenges.deduplication.dedupe_runner_py_stream --runs 0
# python -O -m src.challenges.sectional.section_runner_py_stream --runs 0
python -O -m src.challenges.vanilla.vanilla_runner_py_stream --runs 1 --no-shuffle --size 3_3_1 3_3_10 3_3_100
