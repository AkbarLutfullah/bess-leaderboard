import os
import pandas as pd
from typing import List, Union


def get_asset_ids() -> pd.DataFrame:
    """
    Retrieve xlsx containing all asset IDS for BM and DFR
    :return df: columns:
                site, owner, optimiser, bmu id, dfr/ffr id, mw, mwh
    """
    file_path = os.path.join(os.path.dirname(__file__), "..", "data", "asset_ids.xlsx")
    df = pd.read_excel(file_path, sheet_name="bess bm")

    return df


def convert_columns_to_datetime(df: pd.DataFrame, datetime_columns: List[str],
                                datetime_formats: str = None) -> pd.DataFrame:
    """
    Convert df columns or column to datetime format
    :param df: input dataframe
    :param datetime_columns: columns to convert into datetime format
    :param datetime_formats: datetime format to be specified (optional)
    :return df: df with specified columns converted to datetime
    """
    if datetime_formats is None:
        datetime_formats = [None] * len(datetime_columns)

    for col, fmt in zip(datetime_columns, datetime_formats):
        df[col] = pd.to_datetime(df[col], format=fmt)

    return df


def get_wavg(df: pd.DataFrame, values: str, weights: str) -> Union[float, pd.DataFrame]:
    """
    Get weighted average
    :param df: dataFrame containing the values and weights columns
    :param values: column name containing the values to compute the weighted average
    :param weights: column name containing the weights for the weighted average computation
    :return: weighted average of the specified values column.
    """
    d = df[values]
    w = df[weights]
    return (d * w).sum() / w.sum()
