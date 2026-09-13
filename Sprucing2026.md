# Top 15 Action Items for Sprucing Up the Repository (2026)

To make these repositories approachable, professional, and functional for external third parties, the following cleanup and finalization tasks should be addressed. These range from resolving hardcoded paths that break functionality on other machines, to cleaning up "distracting" development artifacts, and filling in obvious structural gaps.

- [x] **1. Missing Root Documentation**
  The repository root (`C:\src\SparkAggregationMethods`) lacks a top-level `README.md`. A comprehensive README is necessary to orient external parties to the multiple sub-projects (`py_spark`, `python_dask`, `python_single_threaded`, `scala_spark`, etc.), explain the overall goals, and link out to sub-project documentation.

- [x] **2. Remove Hardcoded Local Paths in JSON Configurations**
  The configuration files (`results/challenge_strategy_registration.json` within py_spark, python_dask, and python_single_threaded) contain hardcoded local paths (e.g., `C:\Src\GitHub_Hosted\SparkAggMethods2\...`). These absolute paths will immediately break when an external user runs the code and should be converted to relative paths.

- [x] **3. Fix the Hardcoded `makefile_python.ps1` Path**
  `python_free_threaded\makefile.ps1` explicitly sources `. C:\Src\GitHub_Hosted\SparkAggMethods2\makefile_python.ps1`. This should use `. ..\makefile_python.ps1` as it correctly does in `py_spark\makefile.ps1` and `python_dask\makefile.ps1`.

- [x] **4. Remove Hardcoded Temp Paths in Python Code**
  `py_spark\src\utils\tidy_session_pyspark.py` hardcodes `SPARK_SCRATCH_FOLDER = "D:\\temp\\spark_scratch"`. This will crash for any user without a `D:` drive. It should be made configurable, or updated to use the system's default temporary directory.

- [x] **5. Remove Hardcoded Tool Paths in Scripts**
  `py_spark\config_when_clustered\update_venv_executor.ps1` hardcodes the path `C:\program files\7-zip\7z.exe`. To be professional, it should either rely on the system `PATH` or use a standard native PowerShell compression command (such as `Compress-Archive`).

- [ ] **6. Implement Missing Runners (`TODO: Implement this`)**
  Core test runners are commented out with `# TODO: Implement this` in their respective test suites (e.g., `dedupe_runner_dask` and `sectional_runner_dask` in `perf_test_runner_for_dask.py`, as well as `dedupe_runner_py_st` in `perf_test_runner_for_py_st.py`). These need to be finished or explicitly documented as out of scope.

- [ ] **7. Complete Empty Modules**
  `python_single_threaded\src\challenges\conditional\conditional_runner_py_st.py` contains only `# TODO: Implement the main function` and is completely empty otherwise. It should either be completed or entirely removed so it is not distracting to readers.

- [x] **8. Remove Lingering `DEBUG_ARGS` Blocks**
  Almost all runner files (e.g., `dedupe_runner_pyspark.py`, `vanilla_runner_py_ft.py`) contain an unprofessional `DEBUG_ARGS = None if True else (...)` block at the top. This debug harness should be removed in favor of standard CLI arguments or IDE launch configurations.

- [x] **9. Resolve the Untracked `simple_queued_pipelines` Directory**
  `git status` reveals that the `simple_queued_pipelines/` folder is completely untracked. This embedded repository must be properly committed to the parent repository, added as an official git submodule, or ignored via `.gitignore` if it's being auto-generated or pulled in externally.

- [x] **10. Add `scala_spark` Documentation**
  While the Python directories have some rudimentary README files, the `scala_spark` directory lacks any documentation. It needs basic instructions on how to build, test, and run the Scala code.

- [x] **11. Standardize Project Structure Between Python and Scala**
  The Python directories organize code neatly under a `src/challenges` structure, whereas the `scala_spark` code has `DedupePerfTest` and `VanillaPerfTest` sitting directly at the root. Standardizing the project structure across languages will make the repository significantly more approachable.

- [x] **12. Replace bare `print()` statements with `logging`**
  Production code, such as `python_free_threaded\src\challenges\vanilla\strategies\queue_mediated\vanilla_py_ft_queue_pd_prog_numpy.py`, utilizes bare `print()` statements (e.g., `print(df_result)`, `print("Success!")`). These should be refactored to use the project's standard logger setup (`logger.info()`) to align with the rest of the codebase.

- [ ] **13. Clean Up Commented-Out Code**
  Files like `bi_level_runner_dask.py` and `requirements_ide.txt` contain large blocks of commented-out logic (like `n_workers=LOCAL_NUM_EXECUTORS` configurations and `# pyspark==3.2.1`). This is visually distracting and gives the codebase an unfinished feel; it should be deleted prior to external release.

- [ ] **14. Generalize Installation Instructions**
  `py_spark\readme.md` instructs users to install dependencies to highly specific local drives (e.g., `D:\Programs\spark-3.4.3-bin-hadoop3`). These instructions should be generalized (e.g., `<YOUR_INSTALL_DIR>\spark...` or `C:\path\to\...`) to be universally applicable.

- [x] **15. Correct Typos in Documentation**
  `py_spark\readme.md` contains obvious typos, such as "Sve to D:\Programs\hadoop-3.3.5\bin" instead of "Save to". A brief proofreading/spellchecker pass over all documentation and comments would increase the professional polish of the repo.

- [x] **16. Add a Scala Build System (`sbt` or `Maven`)**
  The `scala_spark` directory currently contains raw `.scala` source files without a build configuration. Adding a standard `build.sbt` or `pom.xml` will allow external contributors to compile and run the code from the command line without needing a specific IDE setup.

- [ ] **17. Automate Dataset Generation**
  The test runners depend on specific datasets (e.g., 10m row parquet files). A unified, documented bootstrap script (e.g., `generate_all_test_data.py`) should be created at the root level so new users can generate all necessary data with a single command.

- [ ] **18. Consolidate and Unify Python Makefiles**
  Every Python sub-project currently has its own `makefile.ps1` and `quick_test_run.ps1`. These should be consolidated into a single root-level task runner (such as a standard `Makefile`, `Justfile`, or `Invoke` script) to eliminate duplicated script logic.

- [ ] **19. Create a Master Benchmark Runner Script**
  Users currently have to `cd` into each framework directory to run its benchmarks. A master script should be implemented at the repository root to automatically trigger the benchmarks sequentially across all engines (PySpark, Dask, Single-Threaded, Scala).

- [ ] **20. Provide a Dockerfile or DevContainer Environment**
  The project has complex local setup requirements on Windows (specific Java JDKs, Hadoop `winutils`, Spark versions). Providing a `.devcontainer` or `Dockerfile` would give new contributors a ready-to-use, isolated environment, drastically lowering the barrier to entry.

- [x] **21. We will consolidate dependencies from the scattered requirements.txt files.**
  The repository has many scattered `requirements.txt` and `requirements_frozen.txt` files. Migrating to a modern dependency management tool `uv` and `pyproject.toml` files will make environment management much more robust.
  Note: `py_spark/config_when_clustered/requirements_driver.txt` and
  `requirements_executor.txt` are intentionally retained — cluster driver and
  executor nodes provision with `pip`, not `uv`, so these files are the
  deployment interface rather than leftovers from the old layout.

- [ ] **22. Add an Automated CI/CD Pipeline (GitHub Actions)**
  There are no automated checks. Setting up a basic GitHub Actions workflow to run `py_compile`, linting, and basic unit tests on PRs will prevent the introduction of regressions or typos.

- [x] **23. Integrate Python Linting and Formatting (Pre-commit)**
  To maintain a professional standard, tools `isort`, 'autopep8', `flake8`, `ty`, `pytest`  should be used by `.pre-commit-config.yaml` hooks to ensure consistent code quality amount the Python files.

- [x] **24. Enforce Static Type Checking (ty)**
  The common harness uses advanced Python typing (`Generics`, `TypeVars`, etc.), but there is no strict static type checking enforced. Running `ty` against the codebase will harden these core utilities against bugs.

- [x] **25. Add Open Source Community Standard Files**
  To be fully approachable as an open-source reference, the root directory should include a `CONTRIBUTING.md`, an explicit `LICENSE` file, and optionally an issue template.

- [x] **26. Fix Global Variable Passing in Makefiles**
  The sub-project `makefile.ps1` scripts dot-source `makefile_python.ps1` and pass variables (`$PythonVersion`, `$SrcFolderName`, `$EditablePackageFolders`) implicitly via global scope. These should be defined as explicit parameters in `makefile_python.ps1` and passed in as command-line arguments.
