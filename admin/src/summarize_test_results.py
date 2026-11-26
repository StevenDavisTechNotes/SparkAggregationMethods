# usage: python -m src.summarize_test_results

import io
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd  # type: ignore[unresolved-import]
import scipy
from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration, ChallengeStrategyRegistrationKeyColumns,
    read_challenge_strategy_registration_consolidated,
)
from spark_agg_methods_common_python.perf_test_common import (
    ENGINE_FOLDER_MAP, TESTING_YEAR, CalcEngine, Challenge, SolutionLanguage,
)
from spark_agg_methods_common_python.utils.pandas_helpers import make_empty_pd_dataframe_from_schema
from spark_agg_methods_common_python.utils.platform import setup_logging

logger = logging.getLogger(__name__)
FINAL_REPORT_FILE_PATH = "../results/{year}/{challenge}_results.csv"


@dataclass(frozen=True)
class SummarizedPerformanceOfMethodAtDataSize:
    challenge: Challenge
    strategy_name: str
    language: SolutionLanguage
    engine: CalcEngine
    environment: str
    interface: str
    regressor: int
    number_of_runs: int
    elapsed_time_avg: float
    elapsed_time_std: float | None
    elapsed_time_rl: float | None
    elapsed_time_rh: float | None


def read_run_result_file(
        *,
        log_file_path: Path | None,
        key_columns: list[str],
        regressor_column: str,
        dependent_variable_column: str,
) -> pd.DataFrame:
    empty_results = make_empty_pd_dataframe_from_schema(
        {
            key_column: str
            for key_column in key_columns
        } |
        {
            regressor_column: float,
            dependent_variable_column: float,
        }
    )
    if log_file_path is None or not log_file_path.exists():
        return empty_results
    result_files: list[pd.DataFrame] = []
    result_files.append(empty_results)

    def parse_segment(
        segment: list[str],
    ) -> pd.DataFrame | None:
        num_lines = len(segment)
        if num_lines == 0:
            # have not read the first line yet
            return None
        if len(segment) <= 1:
            # header with no lines
            return None
        # take the blank off the header
        segment[0] = segment[0].strip()
        segment_df = pd.read_csv(io.StringIO("\n".join(segment)), header=0)
        return segment_df

    with log_file_path.open("r") as f:
        segment: list[str] = []
        for i_line, line in enumerate(f):
            line = line.rstrip()
            if line.startswith("#"):
                logger.info(f"Excluding line: {line} at {i_line}")
                continue
            if line.find(",") < 0:
                logger.info(f"Excluding line: {line} at {i_line}")
                continue
            if line.startswith(" "):
                match parse_segment(segment):
                    case None:
                        pass
                    case segment_df:
                        result_files.append(segment_df)
                segment.clear()
            segment.append(line)
        match parse_segment(segment):
            case None:
                pass
            case segment_df:
                result_files.append(segment_df)
    df = pd.concat(objs=result_files)
    df = (
        df
        .drop(columns=[col for col in df.columns if col.startswith("Unnamed:")])
        .reset_index(drop=True)
    )
    return df


def clean_key_field(
        *,
        key_field: str,
        expected_value: str,
        src_df: pd.DataFrame,
        src_file_path: Path,
) -> pd.DataFrame:
    src_df = src_df.copy()
    file_language = src_df[key_field].dropna().unique().tolist()
    if len(file_language) > 1:
        logger.warning(f"Warning: multiple {key_field} found in {src_file_path}: {file_language}")
    elif 1 == len(file_language):
        actual_value = file_language[0]
        if actual_value != expected_value:
            logger.warning(f"Warning: {key_field} in {src_file_path} is {actual_value} but expected {expected_value}")
    src_df[key_field] = expected_value
    return src_df


def analyze_elapsed_time_samples(
    *,
    challenge: Challenge,
    strategy_name: str,
    language: SolutionLanguage,
    engine: CalcEngine,
    environment: str,
    interface: str,
    regressor_value: int,
    elapsed_time_samples: pd.Series,
) -> SummarizedPerformanceOfMethodAtDataSize:
    confidence = 0.95
    num_runs = len(elapsed_time_samples)
    elapsed_time_avg = cast(float, elapsed_time_samples.mean())
    elapsed_time_std: float | None
    confident_low: float | None
    confident_high: float | None
    if num_runs > 1:
        elapsed_time_std = cast(float, elapsed_time_samples.std(ddof=1))
        confident_low, confident_high = scipy.stats.norm.interval(
            confidence,
            loc=elapsed_time_avg,
            scale=elapsed_time_std / confidence
        )
    else:
        elapsed_time_std = confident_low = confident_high = None
    return SummarizedPerformanceOfMethodAtDataSize(
        challenge=challenge,
        strategy_name=strategy_name,
        language=language,
        engine=engine,
        environment=environment,
        interface=interface,
        regressor=regressor_value,
        number_of_runs=num_runs,
        elapsed_time_avg=elapsed_time_avg,
        elapsed_time_std=elapsed_time_std,
        elapsed_time_rl=confident_low,
        elapsed_time_rh=confident_high,
    )


def summarize_environment_results(
        *,
        challenge: Challenge,
        language: SolutionLanguage,
        engine: CalcEngine,
        environment: str,
        registration: ChallengeResultLogFileRegistration,
        common_key_fields: list[str],
) -> list[SummarizedPerformanceOfMethodAtDataSize]:
    strategies_by_name = {x.strategy_name: x for x in registration.strategies}
    result_file_path = Path("..") / ENGINE_FOLDER_MAP[engine] / registration.result_file_path[environment]
    regressor_column_name = registration.regressor_column_name
    elapsed_time_column_name = registration.elapsed_time_column_name
    df = read_run_result_file(
        log_file_path=result_file_path,
        key_columns=common_key_fields + [regressor_column_name],
        regressor_column=regressor_column_name,
        dependent_variable_column=elapsed_time_column_name,
    )
    clean_regressor_df = df[regressor_column_name].round(0).astype(int)
    if (clean_regressor_df - df[regressor_column_name]).abs().max() > 0:
        logger.warning(f"Warning: regressor values in {result_file_path} are not integers")
    df[regressor_column_name] = clean_regressor_df
    for key_field, expected_value in (("language", language), ("engine", engine), ("challenge", challenge)):
        df = clean_key_field(
            key_field=key_field,
            expected_value=expected_value,
            src_df=df,
            src_file_path=result_file_path,
        )
    summaries: list[SummarizedPerformanceOfMethodAtDataSize] = []
    group_iterator = cast(Any, df.groupby(by=["strategy_name", regressor_column_name]))
    for (strategy_name, regressor_value), group in group_iterator:
        if strategy_name not in strategies_by_name:
            logger.warning(
                f"Warning: strategy {strategy_name} in results but not in challenge registration for "
                f"language {language}, engine {engine}, challenge {challenge}")
            continue
        strategy = strategies_by_name[strategy_name]
        summaries.append(analyze_elapsed_time_samples(
            challenge=Challenge(strategy.challenge),
            strategy_name=strategy_name,
            language=language,
            engine=engine,
            environment=environment,
            interface=strategy.interface,
            regressor_value=regressor_value,
            elapsed_time_samples=cast(pd.Series, group[elapsed_time_column_name]),
        ))
    return summaries


def analyze_run_results():
    challenge_result_log_registrations = read_challenge_strategy_registration_consolidated()
    common_key_fields = [x for x in ChallengeStrategyRegistrationKeyColumns.model_fields]
    for challenge in Challenge:
        summary_status: list[SummarizedPerformanceOfMethodAtDataSize] = []
        for language in challenge_result_log_registrations:
            for engine in challenge_result_log_registrations[language]:
                if challenge not in challenge_result_log_registrations[language][engine]:
                    logger.warning(f"Did not find {challenge.value} in {language.value} {engine.value} registration")
                    continue
                registration = challenge_result_log_registrations[language][engine][challenge]
                for environment in registration.result_file_path:
                    environment_summaries = summarize_environment_results(
                        challenge=challenge,
                        language=language,
                        engine=engine,
                        environment=environment,
                        registration=registration,
                        common_key_fields=common_key_fields,
                    )
                    summary_status.extend(environment_summaries)
                    engine_runs = [x.number_of_runs for x in environment_summaries]
                    if engine_runs:
                        logger.info(
                            f"{challenge.value} on {engine.value} in {environment}: "
                            f"number_of_runs smallest={min(engine_runs)}, largest={max(engine_runs)}")
        print_summary(
            challenge=challenge,
            summary_status=summary_status,
        )


def print_summary(
        challenge: Challenge,
        summary_status: list[SummarizedPerformanceOfMethodAtDataSize],
):
    file_report_file_path = FINAL_REPORT_FILE_PATH.format(
        year=TESTING_YEAR,
        challenge=challenge.name.lower(),
    )
    os.unlink(file_report_file_path) if os.path.exists(file_report_file_path) else None
    if len(summary_status) > 0:
        logger.info(f"Summarizing challenge: {challenge}")
        df = pd.DataFrame(summary_status)
        df = df.sort_values(by=["challenge", "language", "interface", "strategy_name", "environment"])
        df.to_csv(file_report_file_path, index=False)


def main():
    logger.info(f"Running {__file__}")
    analyze_run_results()
    logger.info("Done!")


if __name__ == "__main__":
    setup_logging()
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted!")
