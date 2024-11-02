import json
from pathlib import Path
from typing import Iterable

from pydantic import BaseModel, TypeAdapter

from spark_agg_methods_common_python.perf_test_common import CalcEngine, Challenge, SolutionLanguage

REGISTRY_FILE_PATH = "results/challenge_strategy_registration.json"

CHALLENGE_STRATEGY_REGISTRATION_MAP = {
    CalcEngine.PYTHON_ONLY: "../SparkAggMethods_PythonOnly/results/challenge_strategy_registration.json",
    CalcEngine.DASK: "../SparkAggMethods_PythonDask/results/challenge_strategy_registration.json",
    CalcEngine.PYSPARK: "../SparkAggMethods_PySpark/results/challenge_strategy_registration.json",
    CalcEngine.SCALA_SPARK: "../SparkAggMethods_Scala_Spark/results/challenge_strategy_registration.json",
}


class ChallengeStrategyRegistrationKeyColumns(BaseModel):
    language: str
    engine: str
    challenge: str
    interface: str
    strategy_name: str


class ChallengeStrategyRegistration(ChallengeStrategyRegistrationKeyColumns):
    # ChallengeStrategyRegistrationKeyColumns
    language: str
    engine: str
    challenge: str
    interface: str
    strategy_name: str
    # Additional fields
    numerical_tolerance: float
    requires_gpu: bool


class ChallengeResultLogFileRegistration(BaseModel):
    result_file_path: str
    regressor_column_name: str
    elapsed_time_column_name: str
    strategies: list[ChallengeStrategyRegistration]
    expected_regressor_values: list[int]


ChallengeRegistryFileType = \
    dict[SolutionLanguage,
         dict[CalcEngine,
              dict[Challenge,
                   ChallengeResultLogFileRegistration]]]

CHALLENGE_REGISTRY_TYPE_ADAPTER = TypeAdapter(ChallengeRegistryFileType)


def read_single_repo_challenge_strategy_registration(
        file_path: str
) -> ChallengeRegistryFileType:
    if not Path(file_path).exists():
        return dict()
    text = Path(file_path).read_text()
    return CHALLENGE_REGISTRY_TYPE_ADAPTER.validate_json(text)


def merge_registries(registries: Iterable[ChallengeRegistryFileType]) -> ChallengeRegistryFileType:
    result: ChallengeRegistryFileType = dict()
    for registry in registries:
        for language, engines in registry.items():
            if language not in result:
                result[language] = dict()
            for engine, challenges in engines.items():
                if engine not in result[language]:
                    result[language][engine] = dict()
                for challenge, registration in challenges.items():
                    msg = f"Duplicate challenge {challenge} for {language} and {engine}"
                    assert challenge not in result[language][engine], msg
                    result[language][engine][challenge] = registration
    return result


def read_consolidated_challenge_strategy_registration(
) -> ChallengeRegistryFileType:
    consolidated_data = merge_registries(
        read_single_repo_challenge_strategy_registration(file_path)
        for file_path in CHALLENGE_STRATEGY_REGISTRATION_MAP.values()
    )
    return consolidated_data


def update_challenge_strategy_registration(
        *,
        language: SolutionLanguage,
        engine: CalcEngine,
        challenge: Challenge,
        registration: ChallengeResultLogFileRegistration,
) -> None:
    def read_file() -> ChallengeRegistryFileType:
        return read_single_repo_challenge_strategy_registration(REGISTRY_FILE_PATH)

    def write_file(data: ChallengeRegistryFileType):
        Path(REGISTRY_FILE_PATH).parent.mkdir(parents=True, exist_ok=True)
        with open(REGISTRY_FILE_PATH, "w") as file:
            json.dump({
                lang: {
                    e: {
                        c: file.model_dump()
                        for c, file in challenges.items()
                    }
                    for e, challenges in engines.items()
                }
                for lang, engines in data.items()
            }, file, indent=4)

    def update(data: ChallengeRegistryFileType) -> ChallengeRegistryFileType:
        data = data or dict()
        engines = data[language] = data.get(language) or dict()
        challenges = engines[engine] = engines.get(engine) or dict()
        challenges[challenge] = registration
        return data

    data = read_file()
    data = update(data)
    write_file(data)
