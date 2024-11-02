from dataclasses import is_dataclass
from typing import TypeVar, get_type_hints

import pandas as pd

TDataClassBased = TypeVar("TDataClassBased")
TNamedTupleBased = TypeVar("TNamedTupleBased", bound=tuple)


def is_a_namedtuple_subclass(t_type: type) -> bool:
    return issubclass(t_type, tuple) and hasattr(t_type, '_fields')


def make_empty_pd_dataframe_from_schema(
    schema: dict[str, type],
) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            col_name: pd.Series(data=[], dtype=t_type) for col_name, t_type in schema.items()
        }
    )
    return df


def make_empty_pd_dataframe_for_class(
    row_type: type,
) -> pd.DataFrame:
    schema = get_type_hints(row_type)
    df = pd.DataFrame(
        {
            col_name: pd.Series(data=[], dtype=t_type) for col_name, t_type in schema.items()
        }
    )
    return df


def make_pd_dataframe_from_list_of_named_tuples(
    rows: list[TNamedTupleBased],
    *,
    row_type: type,
) -> pd.DataFrame:
    assert is_a_namedtuple_subclass(row_type)
    schema = get_type_hints(row_type)
    df = pd.DataFrame(
        {
            col_name: pd.Series(
                [row[i_col] for row in rows],
                dtype=t_type,
            )
            for i_col, (col_name, t_type) in enumerate(schema.items())
        }
    )
    return df


def make_pd_dataframe_from_list_of_dataclasses(
    rows: list[TDataClassBased],
    *,
    row_type: type,
) -> pd.DataFrame:
    assert is_dataclass(row_type)
    schema = get_type_hints(row_type)
    df = pd.DataFrame(
        {
            col_name: pd.Series(
                [getattr(row, col_name) for row in rows],
                dtype=t_type,
            )
            for col_name, t_type in schema.items()
        }
    )
    return df
