import json
from pathlib import Path

from pydantic import BaseModel, TypeAdapter

from src.perf_test_common import CalcEngine, Challenge, SolutionLanguage

REGISTRY_FILE_PATH = "./results/challenge_strategy_registration.json"


class ChallengeStrategyRegistration(BaseModel):
    language: str
    engine: str
    challenge: str
    interface: str
    strategy_name: str
    numerical_tolerance: float
    requires_gpu: bool


class ChallengeResultLogFileRegistration(BaseModel):
    result_file_path: str
    regressor_column_name: str
    elapsed_time_column_name: str
    strategies: list[ChallengeStrategyRegistration]
    expected_regressor_values: list[int]


ChallengeRegistryFileType = dict[str, dict[str, dict[str, ChallengeResultLogFileRegistration]]]

CHALLENGE_REGISTRY_TYPE_ADAPTER = TypeAdapter(ChallengeRegistryFileType)


def update_challenge_strategy_registration(
        *,
        language: SolutionLanguage,
        engine: CalcEngine,
        challenge: Challenge,
        registration: ChallengeResultLogFileRegistration,
) -> None:
    def read_file() -> ChallengeRegistryFileType:
        text = Path(REGISTRY_FILE_PATH).read_text()
        return CHALLENGE_REGISTRY_TYPE_ADAPTER.validate_json(text)

    def write_file(data: ChallengeRegistryFileType):
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
