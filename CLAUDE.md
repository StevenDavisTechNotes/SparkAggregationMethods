# CLAUDE.md

Guidance for AI coding assistants working in this repository. Read this before
making changes; it captures the conventions that are not obvious from any single
file.

## What this repository is

A benchmark / reference implementation comparing **five data aggregation
challenges** implemented across **several compute engines** (PySpark, Dask,
single-threaded Python, free-threaded Python, Scala Spark). It is companion code
for [Steven Davis's Tech Notes](https://stevendavistechnotes.blogspot.com/).

The interesting artifact is the *matrix*: challenge x engine x strategy. Most
work involves adding or maintaining a **strategy** — one concrete way of solving
one challenge on one engine — and keeping the surrounding registration and
result-logging plumbing consistent.

Challenges (`Challenge` enum in `perf_test_common.py`): `vanilla`, `bilevel`,
`conditional`, `sectional`, `deduplication`.

## Repository layout

Everything below lives under the repo root, which is also the `uv` project that
packages the shared `spark_agg_methods_common_python` package for the other sub-projects.

| Directory | Purpose | Python |
| --- | --- | --- |
| `.` | Root `uv` project; also packages `common_python/` | 3.10 |
| `admin/` | Whole-repo tooling (data generation, result summarization). Nothing imports it. | 3.14 |
| `common_python/` | Shared types, test harness, registry, domain logic. Package name `spark_agg_methods_common_python`. | 3.10 |
| `py_spark/` | PySpark implementations | 3.10 (Spark 3.5.9) |
| `python_dask/` | Dask implementations | 3.12 (`dask-sql`) |
| `python_single_threaded/` | Pandas / plain Python baselines | 3.14 |
| `python_free_threaded/` | Threaded + queue-mediated implementations | 3.14t (GIL-free) |
| `simple_queued_pipelines/` | Git submodule: streaming pipeline components | 3.14t |
| `scala_spark/` | Scala Spark implementations (`sbt`) | JDK 17 |
| `scripts/` | Repo-wide lint/test runners invoked by pre-commit | root env |
| `results/` | Consolidated, summarized benchmark output | — |

Each Python sub-project is its **own `uv` project** with its own `pyproject.toml`,
`uv.lock`, and `.venv`; they cannot share one lockfile because the Python
versions differ that much. Sub-projects depend on the shared package via
`spark_agg_methods_common_python = { path = "..", editable = true }`.

`common_python/` is also its own `uv` project: its `pyproject.toml` is derived from
the root one (same name, dependencies and dev group, but `where = ["."]`; it deliberately
has no `[tool.isort]`, since isort finds the root one by walking up), and it has
its own `uv.lock` and `.venv`. The root project still packages `common_python/` (plus
`simple_queued_pipelines/`), which is what the other sub-projects' `path = ".."` source
installs, so a dependency change to the shared package must be made in **both**
`pyproject.toml` files.

## Environment

Primary development is on **Windows with PowerShell**, opened via
`python-workspace.code-workspace` (which deliberately omits `scala_spark`).
Prefer PowerShell syntax for scripts; `.ps1` helpers already exist in most
sub-projects.

```powershell
# From the repo root, once per sub-project you intend to run:
uv sync
cd common_python; uv sync; cd ..
cd admin; uv sync; cd ..
cd py_spark; uv sync; cd ..
cd python_dask; uv sync; cd ..
cd python_single_threaded; uv sync; cd ..
cd python_free_threaded; uv sync; cd ..
cd simple_queued_pipelines; uv sync; cd ..
```

If `simple_queued_pipelines/` is empty: `git submodule update --init --recursive`.

PySpark additionally needs JDK 17, a local Spark distribution, and Hadoop
`winutils` on Windows — see `py_spark/README.md` for the environment variables.

Test data location defaults to `%TEMP%\spark_test_data` and is overridable with
the `SPARK_PERF_TESTING_DATA_LOCATION` environment variable
(`perf_test_common.LOCAL_TEST_DATA_FILE_LOCATION`).

## Common commands

```powershell
# Generate the test data files (run once before benchmarking)
cd admin; uv run python -m src.generate_data_files

# Run every benchmark for one engine
cd python_dask; uv run python -O -m src.perf_test_runner_for_dask

# Run one challenge, small sizes, single pass (see quick_test_run.ps1)
uv run python -m src.challenges.vanilla.vanilla_runner_dask --runs 1 --no-shuffle --size 3_3_1 3_3_10

# Refresh the strategy registry without benchmarking (see update_challenge_registry.ps1)
uv run python -m src.challenges.vanilla.vanilla_runner_dask --runs 0

# Summarize accumulated results into results/*.csv
cd admin; uv run python -m src.summarize_test_results

# Repo-wide checks (what pre-commit runs)
uv run python scripts/run_all.py --pytest
uv run python scripts/run_all.py --flake8
uv run python scripts/run_all.py --autopep8
uv run python scripts/run_all.py --ty
# pyright is opt-in (not run by pre-commit, not in the default set); uses the global npm install
# (`npm install -g pyright`) with each project's own .venv
uv run python scripts/run_all.py --pyright
# No tool flag = autopep8, flake8, ty and pytest; --exclude <project> (repeatable) skips a sub-project

# Scala
cd scala_spark; sbt compile; sbt test
```

Runners take a consistent CLI: `--runs`, `--size`, `--strategy`,
`--shuffle/--no-shuffle`, `--random-seed`, plus engine-specific flags
(`--have-gpu` on Dask, `--cloud-mode` on the PySpark dedupe runner).

Run benchmarks with `-O` so assertions do not distort timings.

## Architecture

### The harness

`common_python/spark_agg_methods_common_python/perf_test_common.py` holds the
shared vocabulary: the `Challenge`, `CalcEngine`, `SolutionLanguage`, and
`SolutionInterface*` enums, `ChallengeMethodRegistrationBase`,
`DataSetDescriptionBase`, `RunnerArgumentsBase`, `RunResultBase`, and
`assemble_itinerary`. Engine-specific subclasses live in each sub-project under
`src/challenges/six_field_test_data/`.

### Anatomy of a challenge directory

```txt
<engine>/src/challenges/<challenge>/
    <challenge>_runner_<engine>.py             # CLI entry point + result logging
    <challenge>_strategy_directory_<engine>.py # the strategy registry list
    <challenge>_test_data_types_<engine>.py    # engine-specific dataset types
    domain_logic/                              # shared, engine-specific helpers
    strategies/using_<engine>/                 # one file per strategy
```

Engine suffixes used throughout file and function names: `_pyspark`, `_dask`,
`_py_st` (single threaded), `_py_ft` (free threaded).

### Runner responsibilities

Every `*_runner_*.py` follows the same shape — copy an existing one rather than
inventing a new structure:

1. Module-level `LANGUAGE` / `ENGINE` / `CHALLENGE` constants.
2. `parse_args()` returning a frozen `Arguments(RunnerArgumentsBase)` dataclass,
   with `--size` and `--strategy` choices derived from the data-size list and the
   strategy registry.
3. `prepare_data_sets()`.
4. A `*RunResultFileWriter` subclass declaring `RUN_LOG_FILE_PATH`
   (`results/<challenge>_<engine>_runs.csv`).
5. `do_test_runs()` — walks `assemble_itinerary(args)`, executes one strategy per
   step, verifies the answer within the strategy's numerical tolerance, writes a
   run result, and skips writing for `debugging_only` data sizes.
6. `update_challenge_registration()` — writes this engine's strategy metadata to
   `results/challenge_strategy_registration.json`.
7. `main()` = parse args, update registration, run, log "Done!".

### The strategy registry

`challenge_strategy_registry.py` defines the Pydantic schema for
`results/challenge_strategy_registration.json`, one per engine directory.
`CHALLENGE_STRATEGY_REGISTRATION_MAP` maps each `CalcEngine` to its **relative**
path — keep these relative; hardcoded absolute paths were a known past defect.
`admin/src/summarize_test_results.py` reads the consolidated registry plus each
engine's run logs to produce `results/<challenge>_results.csv`.

Registration is regenerated by running any runner with `--runs 0`, so the JSON is
generated output — change the strategy directory, not the JSON.

## Conventions

Detailed Python and Scala style (layout, naming, typing, paths, logging, CLI patterns) is in
[`instructions/coding_standards.md`](instructions/coding_standards.md); read it before writing
new code. The rules below are the ones the tooling enforces or that are easy to get wrong.

- **Line length 120** (`setup.cfg` for flake8/pycodestyle, `[tool.isort]` in the
  root `pyproject.toml`), max complexity 10.
- **isort** with `multi_line_output = 5` and trailing commas. Import order matters
  to pre-commit; let the tool sort rather than hand-editing. The only config is the root
  `[tool.isort]` (plus the submodule's own copy); isort finds it by walking up from each file, so
  do not pass `--settings-file pyproject.toml` in a sub-project's `.vscode/settings.json`: that
  resolves to the sub-project's section-less `pyproject.toml` and silently falls back to isort defaults.
- **Formatting** is `autopep8`, not black.
- **Type checking** is `ty` at commit time; `pyrightconfig.json` files exist for
  the IDE.
- **Tests** are co-located with the code they test and named `*_test.py`
  (each `pytest.ini` sets `python_files = *_test.py`). There is no separate
  `tests/` tree.
- **Logging, never `print()`** in library or strategy code. Use
  `logger = logging.getLogger(__name__)` and call `setup_logging()` from
  `utils/platform.py` in `__main__` blocks.
- **Frozen dataclasses** for arguments, dataset descriptions, and registrations.
- **Strategy names** come from `name_of_function(...)` rather than string
  literals, so the function name *is* the published strategy name. Renaming a
  strategy function changes benchmark result identity.
- `strategy_name_2018` links a strategy to its name in the original 2018 blog
  posts; it is `None` when there is no counterpart.
- **Spell checking**: `cspell.json` files exist at the root and per sub-project.
  Add genuinely new domain words rather than working around a warning.

## Adding a new strategy

1. Add the implementation under
   `<engine>/src/challenges/<challenge>/strategies/using_<engine>/`, named
   `<challenge>_<engine>_<interface>_<variant>.py`, exporting one function of the
   same name.
2. Append a registration entry in `<challenge>_strategy_directory_<engine>.py`,
   setting `interface`, `numerical_tolerance`, and `requires_gpu`.
3. Run the runner with `--runs 0` to regenerate
   `results/challenge_strategy_registration.json`, and commit that change.
4. Smoke-test on the smallest sizes before running the full benchmark.

## Pre-commit

Hooks are configured in `.pre-commit-config.yaml` and are `fail_fast`. Install
them in both repositories:

```powershell
uv run pre-commit install
cd simple_queued_pipelines; uv run pre-commit install; cd ..
```

On commit: isort, autopep8, flake8, `ty`, and the fast test suite (`py_spark`
excluded). On push: the full test suite.

## In-flight work and known rough edges

This repo is mid-way through a cleanup pass; `instructions/Sprucing2026.md` is the live
checklist. When touching these areas, expect breakage rather than assuming the
current state is intentional:

- Directories were recently renamed from `CamelCase` to `snake_case`. Tooling and
  config have been updated, but `README.md` and `CONTRIBUTING.md` still refer to
  the old names and to a single-`uv sync` setup that no longer matches the
  per-project layout.
- Only `common_python`, `py_spark`, and `simple_queued_pipelines` currently
  contain `*_test.py` files. The other projects are in the sweep but collect
  nothing, which `scripts/run_all.py --pytest` tolerates by accepting pytest exit
  code 5. New strategies mostly arrive untested.
- Several runners are still unimplemented and commented out with
  `# TODO: Implement this` (Dask dedupe and sectional, single-threaded dedupe,
  single-threaded conditional). Leaving them alone is fine; finishing them is
  tracked work, not incidental cleanup.
- Azure Data Lake support exists behind an `in_cloud_mode` flag
  (`--cloud-mode` on the PySpark dedupe runner, `IsCloudMode` in Scala). It was
  only briefly exercised and is not part of routine testing.

Prefer finishing or removing confusing unfinished work over expanding it. Leave
clearly-scoped unfinished work in place unless it makes the code look
unprofessional to an external reader.
