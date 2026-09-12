# Top 15 Action Items for Sprucing Up the Repository (2026)

To make these repositories approachable, professional, and functional for external third parties, the following cleanup and finalization tasks should be addressed. These range from resolving hardcoded paths that break functionality on other machines, to cleaning up "distracting" development artifacts, and filling in obvious structural gaps.

- [x] **1. Missing Root Documentation**
  The repository root (`C:\src\SparkAggregationMethods`) lacks a top-level `README.md`. A comprehensive README is necessary to orient external parties to the multiple sub-projects (`PySpark`, `PythonDask`, `PythonSingleThreaded`, `ScalaSpark`, etc.), explain the overall goals, and link out to sub-project documentation.

- [x] **2. Remove Hardcoded Local Paths in JSON Configurations**
  The configuration files (`results/challenge_strategy_registration.json` within PySpark, PythonDask, and PythonSingleThreaded) contain hardcoded local paths (e.g., `C:\Src\GitHub_Hosted\SparkAggMethods2\...`). These absolute paths will immediately break when an external user runs the code and should be converted to relative paths.

- [x] **3. Fix the Hardcoded `makefile_python.ps1` Path**
  `PythonStreaming\makefile.ps1` explicitly sources `. C:\Src\GitHub_Hosted\SparkAggMethods2\makefile_python.ps1`. This should use `. ..\makefile_python.ps1` as it correctly does in `PySpark\makefile.ps1` and `PythonDask\makefile.ps1`.

- [x] **4. Remove Hardcoded Temp Paths in Python Code**
  `PySpark\src\utils\tidy_session_pyspark.py` hardcodes `SPARK_SCRATCH_FOLDER = "D:\\temp\\spark_scratch"`. This will crash for any user without a `D:` drive. It should be made configurable, or updated to use the system's default temporary directory.

- [x] **5. Remove Hardcoded Tool Paths in Scripts**
  `PySpark\config_when_clustered\update_venv_executor.ps1` hardcodes the path `C:\program files\7-zip\7z.exe`. To be professional, it should either rely on the system `PATH` or use a standard native PowerShell compression command (such as `Compress-Archive`).

- [ ] **6. Implement Missing Runners (`TODO: Implement this`)**
  Core test runners are commented out with `# TODO: Implement this` in their respective test suites (e.g., `dedupe_runner_dask` and `sectional_runner_dask` in `perf_test_runner_for_dask.py`, as well as `dedupe_runner_py_st` in `perf_test_runner_for_py_st.py`). These need to be finished or explicitly documented as out of scope.

- [ ] **7. Complete Empty Modules**
  `PythonSingleThreaded\src\challenges\conditional\conditional_runner_py_st.py` contains only `# TODO: Implement the main function` and is completely empty otherwise. It should either be completed or entirely removed so it is not distracting to readers.

- [x] **8. Remove Lingering `DEBUG_ARGS` Blocks**
  Almost all runner files (e.g., `dedupe_runner_pyspark.py`, `vanilla_runner_py_stream.py`) contain an unprofessional `DEBUG_ARGS = None if True else (...)` block at the top. This debug harness should be removed in favor of standard CLI arguments or IDE launch configurations.

- [x] **9. Resolve the Untracked `SimpleQueuedPipelines` Directory**
  `git status` reveals that the `SimpleQueuedPipelines/` folder is completely untracked. This embedded repository must be properly committed to the parent repository, added as an official git submodule, or ignored via `.gitignore` if it's being auto-generated or pulled in externally.

- [x] **10. Add `ScalaSpark` Documentation**
  While the Python directories have some rudimentary README files, the `ScalaSpark` directory lacks any documentation. It needs basic instructions on how to build, test, and run the Scala code.

- [x] **11. Standardize Project Structure Between Python and Scala**
  The Python directories organize code neatly under a `src/challenges` structure, whereas the `ScalaSpark` code has `DedupePerfTest` and `VanillaPerfTest` sitting directly at the root. Standardizing the project structure across languages will make the repository significantly more approachable.

- [x] **12. Replace bare `print()` statements with `logging`**
  Production code, such as `PythonStreaming\src\challenges\vanilla\strategies\queue_mediated\vanilla_py_mt_queue_pd_prog_numpy.py`, utilizes bare `print()` statements (e.g., `print(df_result)`, `print("Success!")`). These should be refactored to use the project's standard logger setup (`logger.info()`) to align with the rest of the codebase.

- [ ] **13. Clean Up Commented-Out Code**
  Files like `bi_level_runner_dask.py` and `requirements_ide.txt` contain large blocks of commented-out logic (like `n_workers=LOCAL_NUM_EXECUTORS` configurations and `# pyspark==3.2.1`). This is visually distracting and gives the codebase an unfinished feel; it should be deleted prior to external release.

- [ ] **14. Generalize Installation Instructions**
  `PySpark\readme.md` instructs users to install dependencies to highly specific local drives (e.g., `D:\Programs\spark-3.4.3-bin-hadoop3`). These instructions should be generalized (e.g., `<YOUR_INSTALL_DIR>\spark...` or `C:\path\to\...`) to be universally applicable.

- [x] **15. Correct Typos in Documentation**
  `PySpark\readme.md` contains obvious typos, such as "Sve to D:\Programs\hadoop-3.3.5\bin" instead of "Save to". A brief proofreading/spellchecker pass over all documentation and comments would increase the professional polish of the repo.

- [ ] **16. Add a Scala Build System (`sbt` or `Maven`)**
  The `ScalaSpark` directory currently contains raw `.scala` source files without a build configuration. Adding a standard `build.sbt` or `pom.xml` will allow external contributors to compile and run the code from the command line without needing a specific IDE setup.

- [ ] **17. Automate Dataset Generation**
  The test runners depend on specific datasets (e.g., 10m row parquet files). A unified, documented bootstrap script (e.g., `generate_all_test_data.py`) should be created at the root level so new users can generate all necessary data with a single command.

- [ ] **18. Consolidate and Unify Python Makefiles**
  Every Python sub-project currently has its own `makefile.ps1` and `quick_test_run.ps1`. These should be consolidated into a single root-level task runner (such as a standard `Makefile`, `Justfile`, or `Invoke` script) to eliminate duplicated script logic.

- [ ] **19. Create a Master Benchmark Runner Script**
  Users currently have to `cd` into each framework directory to run its benchmarks. A master script should be implemented at the repository root to automatically trigger the benchmarks sequentially across all engines (PySpark, Dask, Single-Threaded, Scala).

- [ ] **20. Provide a Dockerfile or DevContainer Environment**
  The project has complex local setup requirements on Windows (specific Java JDKs, Hadoop `winutils`, Spark versions). Providing a `.devcontainer` or `Dockerfile` would give new contributors a ready-to-use, isolated environment, drastically lowering the barrier to entry.

- [ ] **21. Standardize Python Dependency Management**
  The repository has many scattered `requirements.txt` and `requirements_frozen.txt` files. Migrating to a modern dependency management tool like `uv`, `Poetry`, or a centralized `pyproject.toml` will make environment management much more robust.

- [ ] **22. Add an Automated CI/CD Pipeline (GitHub Actions)**
  There are no automated checks. Setting up a basic GitHub Actions workflow to run `py_compile`, linting, and basic unit tests on PRs will prevent the introduction of regressions or typos.

- [ ] **23. Integrate Python Linting and Formatting (Pre-commit)**
  To maintain a professional standard, tools like `ruff` or `black` + `isort` should be enforced via a `.pre-commit-config.yaml` to ensure consistent code styling across the hundreds of Python files.

- [ ] **24. Enforce Static Type Checking (MyPy)**
  The common harness uses advanced Python typing (`Generics`, `TypeVars`, etc.), but there is no strict static type checking enforced. Running `mypy` against the codebase will harden these core utilities against bugs.

- [ ] **25. Add Open Source Community Standard Files**
  To be fully approachable as an open-source reference, the root directory should include a `CONTRIBUTING.md`, an explicit `LICENSE` file, and optionally an issue template.
