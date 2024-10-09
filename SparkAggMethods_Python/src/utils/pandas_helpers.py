import pandas as pd


def make_empty_pd_dataframe_from_schema(
    schema: dict[str, type],
) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            col_name: pd.Series(data=[], dtype=t_type) for col_name, t_type in schema.items()
        }
    )
    return df
