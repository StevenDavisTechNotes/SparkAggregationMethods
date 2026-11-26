#!python
# usage: uv run python -O -m src.perf_test_runner_for_py_all
#
# Advances the whole result set by one sample per (strategy, data size):
# 1. summarize results/<year>/*.csv and find the smallest and largest number_of_runs,
# 2. run every engine's perf_test_runner_for_*.py with --target-num-samples <smallest + 1>, each one's full output
#    going to a temporary file in $env:TEMP,
# 3. scan that output for errors. Known PySpark-on-Windows flakiness is tolerated; any other error is printed and
#    stops the run (exit code 1),
# 4. summarize again.
import logging
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pandas as pd  # type: ignore[unresolved-import]
from spark_agg_methods_common_python.perf_test_common import TESTING_ENVIRONMENT, TESTING_YEAR
from spark_agg_methods_common_python.utils.platform import setup_logging
from src.summarize_test_results import analyze_run_results

logger = logging.getLogger(__name__)

ADMIN_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ADMIN_ROOT.parent
SUMMARY_FILE_PATTERN = f"../results/{TESTING_YEAR}/*_results.csv"
MAX_ERROR_BLOCK_LINES = 60


@dataclass(frozen=True)
class EngineRunner:
    folder: str
    module: str
    is_pyspark: bool = False


# cheapest first, so that a real problem surfaces before the hours of PySpark work
ENGINE_RUNNERS = (
    EngineRunner(folder="python_free_threaded", module="src.perf_test_runner_for_py_ft"),
    EngineRunner(folder="python_single_threaded", module="src.perf_test_runner_for_py_st"),
    EngineRunner(folder="python_dask", module="src.perf_test_runner_for_dask"),
    EngineRunner(folder="py_spark", module="src.perf_test_runner_for_pyspark", is_pyspark=True),
)

# A line that starts a log record, and so ends the continuation lines of the previous error.
LOG_RECORD_START = re.compile(r"^(?:(?:DEBUG|INFO|WARNING|ERROR|CRITICAL):|\d\d/\d\d/\d\d \d\d:\d\d:\d\d )")
# Lines that start something to report. Spark's own "ERROR Executor" / "ERROR TaskSetManager" records are task
# attempts that Spark retries; a step that finally fails is reported by tidy_session_pyspark.
ERROR_START = re.compile(
    r"^(?:Traceback \(most recent call last\):"
    r"|(?:ERROR|CRITICAL):"
    r"|\d\d/\d\d/\d\d \d\d:\d\d:\d\d ERROR tidy_session_pyspark:)"
)
CHILD_START = re.compile(r"^INFO:__main__:Child \d+/\d+ of ")
CHILD_FAILED = re.compile(r"^ERROR:__main__:Child failed:")
CHILDREN_SUMMARY = re.compile(r"^ERROR:__main__:\d+ child processes failed:")
# What PySpark on Windows does when its Python workers or sockets give out (see py_spark/README.md).
FLAKY_PYSPARK = re.compile(
    r"Python worker exited unexpectedly"
    r"|No buffer space available"
    r"|An established connection was aborted"
    r"|Connection reset"
    r"|Connection refused"
    r"|Py4JNetworkError"
    r"|Answer from Java side is empty"
    r"|java\.net\.SocketException"
    r"|Python worker failed to connect back"
)


@dataclass(frozen=True)
class FoundError:
    flaky: bool
    text: str


def find_errors(lines: list[str], is_pyspark: bool) -> list[FoundError]:
    """The errors in one engine runner's output, each marked as known PySpark flakiness or not.

    A "Child failed" record only says that a child process exited non-zero, so it takes the verdict of the errors
    its child logged: flaky if all were flaky, and not flaky if it logged none that explain the failure.
    """
    errors: list[FoundError] = []
    child_errors: list[FoundError] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        index += 1
        if CHILD_START.match(line):
            child_errors = []
            continue
        if CHILDREN_SUMMARY.match(line):
            continue
        if CHILD_FAILED.match(line):
            if not child_errors:
                errors.append(FoundError(flaky=False, text=f"{line}\n(no error found in the child's output)"))
            continue
        if not ERROR_START.match(line):
            continue
        block = [line]
        while index < len(lines) and len(block) < MAX_ERROR_BLOCK_LINES and not LOG_RECORD_START.match(lines[index]):
            block.append(lines[index])
            index += 1
        text = "\n".join(block)
        found = FoundError(flaky=is_pyspark and FLAKY_PYSPARK.search(text) is not None, text=text)
        errors.append(found)
        child_errors.append(found)
    return errors


def summarize_run_count_range() -> tuple[int, int]:
    """(smallest, largest) number_of_runs over every summarized (strategy, data size) of this environment.

    (0, 0) with no summary yet.
    """
    counts: list[int] = []
    for path in sorted(Path().glob(SUMMARY_FILE_PATTERN)):
        summary = pd.read_csv(path)
        number_of_runs_for_testing_environment = \
            summary.loc[summary["environment"] == TESTING_ENVIRONMENT, "number_of_runs"]
        counts.extend(int(x) for x in number_of_runs_for_testing_environment)
    return (min(counts), max(counts)) if counts else (0, 0)


def run_engine(runner: EngineRunner, target_num_samples: int) -> Path:
    output_dir = os.environ.get("TEMP", tempfile.gettempdir())
    with tempfile.NamedTemporaryFile(
            mode="w", prefix=f"{runner.folder}_", suffix=".log", dir=output_dir, delete=False,
    ) as output_file:
        output_path = Path(output_file.name)
        logger.info(f"Running {runner.module} --target-num-samples {target_num_samples}; output in {output_path}")
        return_code = subprocess.run(
            ["uv", "run", "python", "-O", "-m", runner.module, "--target-num-samples", str(target_num_samples)],
            cwd=REPO_ROOT / runner.folder,
            stdout=output_file,
            stderr=subprocess.STDOUT,
            check=False,
        ).returncode
    logger.info(f"{runner.module} exited with {return_code}")
    return output_path


def main() -> None:
    logger.info(f"Running {__file__}")
    os.chdir(ADMIN_ROOT)  # summarize_test_results addresses ../results relative to admin
    analyze_run_results()
    smallest_num_runs, largest_num_runs = summarize_run_count_range()
    target_num_samples = smallest_num_runs + 1
    logger.info(
        f"number_of_runs ranges from {smallest_num_runs} to {largest_num_runs}; "
        f"targeting {target_num_samples} samples")
    for runner in ENGINE_RUNNERS:
        output_path = run_engine(runner, target_num_samples)
        errors = find_errors(
            lines=output_path.read_text(errors="replace").splitlines(),
            is_pyspark=runner.is_pyspark,
        )
        real_errors = [x for x in errors if not x.flaky]
        if real_errors:
            print(f"Error running {runner.module} (full output in {output_path}):")
            for error in real_errors:
                print(error.text)
            sys.exit(1)
        if errors:
            logger.warning(f"{runner.module}: ignored {len(errors)} known PySpark flakiness errors")
        output_path.unlink()  # kept only when a real error needs investigating
    analyze_run_results()
    smallest_num_runs, largest_num_runs = summarize_run_count_range()
    logger.info(f"number_of_runs now ranges from {smallest_num_runs} to {largest_num_runs}")
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
