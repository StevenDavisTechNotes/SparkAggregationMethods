from typing import TypeVar

MATCH_THRESHOLD = 0.9
# must be 0.4316546762589928 < MATCH_THRESHOLD < 0.9927007299270073 @ 10k

T = TypeVar('T', bound=float | str)


def min_not_null(
        lst: list[T | None]
) -> T | None:
    filtered_list: list[T] = [
        x for x in lst if x is not None]
    return min(filtered_list) \
        if len(filtered_list) > 0 else None


def first_not_null(
        lst: list[T | None]
) -> T | None:
    for x in lst:
        if x is not None:
            return x
    return None


def is_match(
        iFirstName: str,
        jFirstName: str,
        iLastName: str,
        jLastName: str,
        iZipCode: str,
        jZipCode: str,
        iSecretKey: int,
        jSecretKey: int,
) -> bool:
    from difflib import SequenceMatcher
    actualRatioFirstName = SequenceMatcher(
        None, iFirstName, jFirstName) \
        .ratio()
    if actualRatioFirstName < MATCH_THRESHOLD:
        if iSecretKey == jSecretKey:
            raise Exception(
                "FirstName non-match "
                f"for {iSecretKey} with itself "
                f"{iFirstName} {jFirstName} with "
                f"actualRatioFirstName={actualRatioFirstName}")
        return False
    actualRatioLastName = SequenceMatcher(
        None, iLastName, jLastName) \
        .ratio()
    if actualRatioLastName < MATCH_THRESHOLD:
        if iSecretKey == jSecretKey:
            raise Exception(
                "LastName non-match "
                f"for {iSecretKey} with itself "
                f"{iLastName} {jLastName} with "
                f"actualRatioLastName={actualRatioLastName}")
        return False
    if iSecretKey != jSecretKey:
        raise Exception(f"""
        False match for {iSecretKey}-{jSecretKey} with itself
        iFirstName={iFirstName} jFirstName={jFirstName} actualRatioFirstName={actualRatioFirstName}
        iLastName={iLastName} jLastName={jLastName} actualRatioLastName={actualRatioLastName}""")
    return True


def match_single_name(
        lhs: str,
        rhs: str,
        iSecretKey: int,
        jSecretKey: int,
) -> bool:
    from difflib import SequenceMatcher
    actualRatio = SequenceMatcher(
        None, lhs, rhs) \
        .ratio()
    if actualRatio < MATCH_THRESHOLD:
        if iSecretKey == jSecretKey:
            raise Exception(
                f"Name non-match for {iSecretKey} with itself {lhs} {rhs} ratio={actualRatio}")
        return False
    return True
