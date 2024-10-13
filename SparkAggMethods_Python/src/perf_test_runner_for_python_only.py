#!python
# usage: python -m src.perf_test_runner_for_python_only

from src.challenges.bi_level import bi_level_python_only_runner
# from src.challenges.conditional import conditional_python_only_runner
# from src.challenges.deduplication import dedupe_python_only_runner
# from src.challenges.sectional import section_python_only_runner
from src.challenges.vanilla import vanilla_python_only_runner

if __name__ == "__main__":
    print(f"Running {__file__}")
    bi_level_python_only_runner.main()
    # conditional_python_only_runner.main()
    # dedupe_python_only_runner.main()
    # section_python_only_runner.main()
    vanilla_python_only_runner.main()
