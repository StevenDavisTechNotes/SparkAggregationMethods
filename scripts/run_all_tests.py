import argparse
import subprocess
import sys


def run_tests():
    parser = argparse.ArgumentParser()
    parser.add_argument("--exclude", action="append", default=[], help="Folders to exclude from testing")
    args, unknown = parser.parse_known_args()

    targets = [
        ("admin", "src"),
        ("common_python", "spark_agg_methods_common_python"),
        ("py_spark", "src"),
        ("python_dask", "src"),
        ("python_free_threaded", "src"),
        ("python_single_threaded", "src"),
        ("simple_queued_pipelines", "simple_queued_pipelines"),
    ]

    failed = False
    for folder, src in targets:
        if folder in args.exclude:
            print(f"\n{'='*50}\nSkipping tests in {folder}\n{'='*50}", flush=True)
            continue

        print(f"\n{'='*50}\nRunning tests in {folder}\n{'='*50}", flush=True)
        result = subprocess.run(["uv", "run", "python", "-m", "pytest", src], cwd=folder)
        if result.returncode not in (0, 5):
            failed = True
            print(f"Tests failed in {folder}!", file=sys.stderr)

    if failed:
        sys.exit(1)
    else:
        print("\nAll tests passed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    run_tests()

