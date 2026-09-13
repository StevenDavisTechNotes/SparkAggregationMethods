# Cluster node provisioning

`requirements_driver.txt` and `requirements_executor.txt` are intentionally
kept as plain pip requirements files, not migrated to `uv`/`pyproject.toml`.
Spark cluster driver and executor nodes provision with `pip`, so these files
are the deployment interface for that provisioning, not leftovers from the
pre-`uv` layout.
