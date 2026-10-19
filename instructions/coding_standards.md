# Coding standards

<!-- cSpell: ignore scalafmt, println -->

Tooling-enforced rules (line length, isort, autopep8, flake8, `ty`) are in `CLAUDE.md` under
"Conventions". This file covers what the tools do not.

## Python

### Layout and formatting

- Line length 120, flake8 max complexity 10 (`setup.cfg`).
- Four-space indent. Multi-line signatures and calls put **one parameter per line, indented
  eight spaces** (a continuation indent), with the closing `)` back at the `def` indent:

  ```python
  def vanilla_pyspark_rdd_reduce(
          spark_session: TidySparkSession,
          exec_params: SixTestExecutionParameters,
          data_set: SixFieldDataSetPyspark,
  ) -> TSixFieldChallengePendingAnswerPythonPyspark:
  ```

- Trailing commas on multi-line calls, signatures and collection literals.
- Two blank lines between top-level definitions.
- Fluent chains (Spark/pandas) are wrapped in parentheses, one call per line, leading dot.
- Imports are isort-formatted (grid, trailing comma, `multi_line_output = 5`); do not
  hand-sort. Imports are absolute: `spark_agg_methods_common_python...` for shared code and
  `src...` for sub-project code.
- String quotes: prefer double quotes, but use single quotes to avoid escaping.

### Typing

- Annotate every parameter and return type, including `-> None`. Use built-in generics
  (`list[str]`, `dict[str, Path]`, `X | None`) rather than `typing.List`/`Optional`.
- Prefer types from `collections.abc` over `typing`.
- Module-level constants are annotated and `UPPER_SNAKE_CASE`
  (`GROUP_BY_COLUMNS: list[str] = [...]`).
- Pattern-match results with `match`/`case` when a function returns a union (for example
  `RunResultBase | tuple["infeasible", str]`).

### Naming

- Modules, functions, variables: `snake_case`. Classes: `PascalCase`. Test classes:
  `Test_<Subject>`.
- Engine suffixes: `_pyspark`, `_dask`, `_py_st`, `_py_ft`.
- Strategy files and functions: `<challenge>_<engine>_<interface>_<variant>`; the function
  name equals the file name and is the published strategy name (via `name_of_function`).
- Private helpers in a module start with `_` (`_generate_answer_file`).
- Data-size lists come in pairs: `DATA_SIZE_LIST_<CHALLENGE>_MAXIMAL` and
  `..._MINIMAL` (the minimal list is a subset used for code quality maintenance; the maximal list is used for performance testing).

### Data and paths

- **Frozen dataclasses** for arguments, descriptions, registrations and parameter bundles.
- **`pathlib.Path`** for every filesystem path. Path-deriving helpers in `common_python` return
  `Path` (or a frozen dataclass of `Path`s). Use `.exists()`, `.unlink(missing_ok=True)`,
  `.open()`, `.replace()` and `.parent.mkdir(parents=True, exist_ok=True)` rather than `os`/
  `os.path`.  PySpark needs string paths.
- **Atomic persistence** Write files via a temp path (`temp_file=True` variant) then `replace` to the final path.
- Repo-relative paths in registries stay relative (`results/<challenge>_<engine>_runs.csv`).

### Logging

- `logger = logging.getLogger(__name__)` at module level; never `print()` in library or
  strategy code. Call `setup_logging()` only under `if __name__ == "__main__":`.
- Use f-strings or `str.format` for messages; Spark runners log through
  `spark_session.logger`.
- **Every file a generator writes gets a `logger.info(f"Creating {path}")`**, logged before the
  write starts. Directory creation and removal are not logged.

### Command-line entry points

- `argparse`, with a `parse_args()` that returns a frozen `Arguments` dataclass, and a
  `main()` that takes or builds it. `__main__` block: `setup_logging()`, then `main()` inside
  `try/except KeyboardInterrupt` logging `"Interrupted!"`.
- Boolean flags use `argparse.BooleanOptionalAction` (`--shuffle/--no-shuffle`) with an
  explicit `default`.
- Runner size selection:
  - `--size-minimal` (`BooleanOptionalAction`, default `False`);
  - `--size` with `choices=` the maximal codes, `nargs="+"`, **no default**;
  - resolution order: explicit `--size`, then `--size-minimal`, then all maximal sizes.
    Runners that previously skipped `debugging_only` sizes by default (Dask bi-level,
    PySpark dedupe) keep that filter in the fallback.
- Data-generation scripts (`admin/src/generate_data_files.py` and `py_spark/src/generate_data_files_pyspark.py`) take `--overwrite` and `--minimal` (both default `False`). Generator functions accept keyword-only `overwrite: bool = False, minimal: bool = False`.

### Structure

- One strategy per file, one exported function per file.
- Shared logic goes in one of the folders depending upon how reusable:
  - `common_python/spark_agg_methods_common_python/utils/`
  - `common_python/spark_agg_methods_common_python/challenges/`
  - `{engine}/src/utils/`
  - `{engine}/src/challenges/{challenge}/domain_logic`
- Registries are module-level lists of frozen registration objects, built with
  `name_of_function(...)`, one keyword argument per line, in a fixed order
  (`strategy_name_2018`, `strategy_name`, `language`, `engine`, `interface`, `requires_gpu`,
  `numerical_tolerance`, `delegate`).
- Spark/RDD strategies use small module-level named helpers (`create_combiner_2`,
  `merge_value_2`, `final_analytics_2`) instead of long lambdas; short `lambda`s are fine
  inside a chain.
- `# cSpell: ignore ...` comments sit next to the word they excuse.
  - File-level excuses are used when the word is repeated and in markdown files.
- `# type: ignore` / `# ty: ignore[...]` always carries the specific reason or rule.
- Plain `assert` statements are used as safer type hints and when code algorithm is particularly fragile to an assumption. For other cases, a meaningful message should be interpolated.

### Tests

- Co-located `*_test.py`, `pytest`, methods named `test_<behavior>`, and classes named `Test_<Subject>`. Seed random generators (`np.random.seed(0)`) for determinism.

## Scala

The Scala code (`scala_spark/`, Scala 2.12.18, Spark 3.5.9, `sbt`) is older and less
uniform than the Python. There is no `scalafmt` config. Follow the dominant style below
and match the file you are editing.

### Layout

- Sources live directly under `scala_spark/src/` (`Compile / scalaSource` is overridden):
  `challenges/<challenge>/` and `common/`. Package declaration mirrors the directory
  (`package challenges.vanilla`, `package common`).
- One `object` per strategy (`VanillaRDDReduce`, `MethodRDDGroupBy`) exposing a `run(...)`
  with the shared delegate signature; `Runner.scala` holds the registry and `main`.
- Imports: `java.*`, then `scala.*`, then third-party (`org.apache...`), then project
  packages (`common.FileHelper`), separated by blank lines. Group braces are spelled
  `{SparkConf, SparkContext}`; both `{ RDD }` and `{RDD}` appear, prefer no padding.

### Formatting

- Two-space indent. Continuation lines for parameter lists are indented four spaces:

  ```scala
  def run(
      dataSize: Long,
      listData: IndexedSeq[DataPoint],
      sc: SparkContext,
      spark: SparkSession): (RDD[AggResult], DataFrame, Dataset[AggResult]) = {
  ```

- Case-class fields: one per line; column-align the types when the class is a wide record
  (`FirstName:     String`), otherwise a single space after the colon.
- Spark pipelines: one call per line, leading dot, two- or four-space extra indent.
- Remove trailing whitespace on lines you touch (the existing files carry a lot of it).
- Braces on the same line as the `def`/`if`/`else`; `if (cond)` with a space.

### Naming and types

- `PascalCase` for objects, classes and case classes; `camelCase` for `val`/`def`.
- Case classes are `final case class`. Result and domain records are immutable; mutable
  `var` is limited to small helpers (`FileHelper.openAppend`).
- Column and field names that must match the Python/CSV schemas keep their original casing
  (`running_sum_of_C`, `mean_of_C`, `FirstName`); do not camel-case them.
- Prefer explicit return types on public `def`s. Wrap shared tuples in a named case class
  (`ComputationMethod`, `ItineraryStop`, `RunResult`) rather than passing bare tuples.
- Prefer `Option` over `null` in new code. Existing strategies return `(rdd, null, null)`
  for the unused slots of the delegate tuple; keep that convention only where the delegate
  signature requires it.

### Logging and errors

- Log through a `Serializable` holder with a `@transient lazy val log =
  Logger.getLogger(getClass.getName)` (`LogHolder`) so it can be used inside closures. Do
  not use `println` in library code.
- Fail loudly with an exception carrying the path or name involved
  (`throw new Exception(s"Unable to delete ${file.getAbsolutePath}")`).
- Use string interpolation (`s"..."`) rather than concatenation.

### Spark specifics

- Strategy names mirror the Python ones where a counterpart exists; keep the same
  statistics definitions (sample variance with `count - 1`, `NaN` for undefined results).
- Spark and shapeless dependencies are declared in `build.sbt`; Spark is `% "provided"`.
- Cloud (Azure Data Lake) behavior stays behind the `IsCloudMode` flag.
