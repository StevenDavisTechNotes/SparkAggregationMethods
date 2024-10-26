import io
import os

import pandas as pd
import scipy
from spark_agg_methods_common_python.challenge_strategy_registry import (
    ChallengeResultLogFileRegistration,
    ChallengeStrategyRegistrationKeyColumns,
    read_consolidated_challenge_strategy_registration)
from spark_agg_methods_common_python.perf_test_common import (
    CalcEngine, Challenge, SolutionLanguage,
    SummarizedPerformanceOfMethodAtDataSize)
from spark_agg_methods_common_python.utils.pandas_helpers import \
    make_empty_pd_dataframe_from_schema

FINAL_REPORT_FILE_PATH = '../results/{challenge}_results_new.csv'


def read_run_result_file(
        *,
        log_file_path: str | None,
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
    if log_file_path is None or not os.path.exists(log_file_path):
        return empty_results
    results: list[pd.DataFrame] = []
    results.append(empty_results)

    def parse_segment(
        segment: list[str],
    ) -> pd.DataFrame | None:
        num_lines = len(segment)
        if num_lines == 0:
            # haven't read the first line yet
            return None
        if len(segment) <= 1:
            # header with no lines
            return None
        # take the blank off the header
        segment[0] = segment[0].strip()
        df_segment = pd.read_csv(io.StringIO('\n'.join(segment)), header=0)
        return df_segment

    with open(log_file_path, 'r') as f:
        segment: list[str] = []
        for i_line, line in enumerate(f):
            line = line.rstrip()
            if line.startswith('#'):
                print(f"Excluding line: {line} at {i_line}")
                continue
            if line.find(',') < 0:
                print(f"Excluding line: {line} at {i_line}")
                continue
            if line.startswith(' '):
                match parse_segment(segment):
                    case None:
                        pass
                    case df_segment:
                        results.append(df_segment)
                segment.clear()
            segment.append(line)
        match parse_segment(segment):
            case None:
                pass
            case df_segment:
                results.append(df_segment)
    df = pd.concat(objs=results)
    df = (
        df
        .drop(columns=[col for col in df.columns if col.startswith('Unnamed:')])
        .reset_index(drop=True)
    )
    return df


def clean_key_field(
        *,
        key_field: str,
        expected_value: str,
        src_df: pd.DataFrame,
        challenge_result_log_registration: ChallengeResultLogFileRegistration,
) -> pd.DataFrame:
    src_df = src_df.copy()
    file_language = src_df[key_field].dropna().unique().tolist()
    src_file_path = challenge_result_log_registration.result_file_path
    if len(file_language) > 1:
        print(f"Warning: multiple {key_field} found in {src_file_path}: {file_language}")
    elif 1 == len(file_language):
        actual_value = file_language[0]
        if actual_value != expected_value:
            print(f"Warning: {key_field} in {src_file_path} is {actual_value} but expected {expected_value}")
    src_df[key_field] = expected_value
    return src_df


def analyze_elapsed_time_samples(
    *,
    challenge: Challenge,
    strategy_name: str,
    language: SolutionLanguage,
    engine: CalcEngine,
    interface: str,
    regressor_value: int,
    elapsed_time_samples: pd.Series,
) -> SummarizedPerformanceOfMethodAtDataSize:
    confidence = 0.95
    num_runs = len(elapsed_time_samples)
    elapsed_time_avg = elapsed_time_samples.mean()
    if num_runs > 1:
        elapsed_time_std = elapsed_time_samples.std(ddof=1)
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
        interface=interface,
        regressor=regressor_value,
        number_of_runs=num_runs,
        elapsed_time_avg=elapsed_time_avg,
        elapsed_time_std=elapsed_time_std,
        elapsed_time_rl=confident_low,
        elapsed_time_rh=confident_high,
    )


def analyze_run_results():
    challenge_result_log_registrations = read_consolidated_challenge_strategy_registration()
    common_key_fields = [x for x in ChallengeStrategyRegistrationKeyColumns.model_fields]
    for challenge in Challenge:
        summary_status: list[SummarizedPerformanceOfMethodAtDataSize] = []
        for language in challenge_result_log_registrations:
            for engine in challenge_result_log_registrations[language]:
                if challenge not in challenge_result_log_registrations[language][engine]:
                    continue
                challenge_result_log_registration = challenge_result_log_registrations[language][engine][challenge]
                result_file_path = challenge_result_log_registration.result_file_path
                regressor_column_name = challenge_result_log_registration.regressor_column_name
                elapsed_time_column_name = challenge_result_log_registration.elapsed_time_column_name
                df = read_run_result_file(
                    log_file_path=result_file_path,
                    key_columns=common_key_fields + [regressor_column_name],
                    regressor_column=regressor_column_name,
                    dependent_variable_column=challenge_result_log_registration.elapsed_time_column_name,
                )
                df_clean_regressor = df[regressor_column_name].round(0).astype(int)
                if (df_clean_regressor - df[regressor_column_name]).abs().max() > 0:
                    print(f"Warning: regressor values in {result_file_path} are not integers")
                df[regressor_column_name] = df_clean_regressor
                df = clean_key_field(
                    key_field="language",
                    expected_value=language,
                    src_df=df,
                    challenge_result_log_registration=challenge_result_log_registration,
                )
                df = clean_key_field(
                    key_field="engine",
                    expected_value=engine,
                    src_df=df,
                    challenge_result_log_registration=challenge_result_log_registration,
                )
                df = clean_key_field(
                    key_field="challenge",
                    expected_value=challenge,
                    src_df=df,
                    challenge_result_log_registration=challenge_result_log_registration,
                )
                print(df)
                for (strategy_name, regressor_value), group in df.groupby(by=['strategy_name', regressor_column_name]):
                    strategy = next(
                        x for x in challenge_result_log_registration.strategies if x.strategy_name == strategy_name)
                    if strategy is None:
                        print(f"Warning: strategy {strategy_name} in results but not in challenge registration for "
                              f"language {language}, engine {engine}, challenge {challenge}")
                        continue
                    elapsed_time_samples: pd.Series[float] = group[elapsed_time_column_name]
                    summary_status.append(analyze_elapsed_time_samples(
                        challenge=Challenge(strategy.challenge),
                        strategy_name=strategy_name,
                        language=language,
                        engine=engine,
                        interface=strategy.interface,
                        regressor_value=regressor_value,
                        elapsed_time_samples=elapsed_time_samples,
                    ))
        print_summary(challenge, summary_status)


def print_summary(
        challenge: Challenge,
        summary_status: list[SummarizedPerformanceOfMethodAtDataSize],
):
    file_report_file_path = FINAL_REPORT_FILE_PATH.format(challenge=challenge.name.lower())
    os.unlink(file_report_file_path) if os.path.exists(file_report_file_path) else None
    if len(summary_status) > 0:
        print(f"Summary for challenge: {challenge}")
        df = pd.DataFrame(summary_status)
        df = df.sort_values(by=['challenge', 'language', 'interface', 'strategy_name'])
        print(df)
        df.to_csv(file_report_file_path, index=False)


if __name__ == "__main__":
    print(f"Running {__file__}")
    analyze_run_results()
    print("Done!")
