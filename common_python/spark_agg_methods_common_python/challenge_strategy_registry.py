import json
from pathlib import Path
from typing import Iterable

from pydantic import BaseModel, TypeAdapter
from spark_agg_methods_common_python.perf_test_common import \
    ENGINE_FOLDER_MAP, CalcEngine, Challenge, SolutionLanguage, construct_result_file_path

REGISTRY_FILE_NAME = "challenge_strategy_registration.json"


class ChallengeStrategyRegistrationKeyColumns(BaseModel):
    language: str
    engine: str
    challenge: str
    interface: str
    strategy_name: str


class ChallengeStrategyRegistration(ChallengeStrategyRegistrationKeyColumns):
    numerical_tolerance: float
    requires_gpu: bool


class ChallengeResultLogFileRegistration(BaseModel):
    result_file_path: str  # str not Path since meant to be archival
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
        file_path: Path,
) -> ChallengeRegistryFileType:
    if not file_path.exists():
        return dict()
    text = file_path.read_text()
    return CHALLENGE_REGISTRY_TYPE_ADAPTER.validate_json(text)


def read_challenge_strategy_registration_consolidated(
) -> ChallengeRegistryFileType:
    registries: list[ChallengeRegistryFileType] = []
    for folder_name in ENGINE_FOLDER_MAP.values():
        registry_file_path = Path("..") / folder_name / construct_result_file_path(REGISTRY_FILE_NAME)
        registry = read_single_repo_challenge_strategy_registration(registry_file_path)
        registries.append(registry)
    consolidated_data = merge_registries(registries)
    return consolidated_data


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
                    if challenge in result[language][engine]:
                        raise ValueError(
                            f"Duplicate challenge {challenge} for {language} and {engine}"
                        )
                    result[language][engine][challenge] = registration
    return result


def update_challenge_strategy_registration_single_repo(
        *,
        language: SolutionLanguage,
        engine: CalcEngine,
        challenge: Challenge,
        registration: ChallengeResultLogFileRegistration,
) -> None:
    registry_file_path = construct_result_file_path(REGISTRY_FILE_NAME)

    def read_file() -> ChallengeRegistryFileType:
        return read_single_repo_challenge_strategy_registration(registry_file_path)

    def write_file(data: ChallengeRegistryFileType):
        registry_file_path.parent.mkdir(parents=True, exist_ok=True)
        with registry_file_path.open("w") as file:
            json.dump(
                {
                    lang: {
                        e: {
                            c: file.model_dump()
                            for c, file in challenges.items()
                        }
                        for e, challenges in engines.items()
                    }
                    for lang, engines in data.items()
                },
                file,
                indent=4,
            )
            print("", file=file)

    def update(data: ChallengeRegistryFileType) -> ChallengeRegistryFileType:
        data = data or dict()
        engines = data[language] = data.get(language) or dict()
        challenges = engines[engine] = engines.get(engine) or dict()
        challenges[challenge] = registration
        return data

    data = read_file()
    data = update(data)
    write_file(data)
