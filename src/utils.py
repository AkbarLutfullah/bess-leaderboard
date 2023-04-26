import os
import pandas as pd
import streamlit as st
from typing import List, Union
from st_aggrid import AgGrid, ColumnsAutoSizeMode, JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import plotly_express as px


def get_asset_ids() -> pd.DataFrame:
    """
    Retrieve xlsx containing all asset IDS for BM and DFR
    :return df: columns:
                site, owner, optimiser, bmu id, dfr/ffr id, mw, mwh
    """
    file_path = os.path.join(os.path.dirname(__file__), "..", "data", "asset_ids.xlsx")
    df = pd.read_excel(file_path, sheet_name="bess bm")

    return df


def convert_columns_to_datetime(
        df: pd.DataFrame, datetime_columns: List[str], datetime_formats: str = None
) -> pd.DataFrame:
    """
    Convert df columns or column to datetime format
    :param df: input dataframe
    :param datetime_columns: columns to convert into datetime format
    :param datetime_formats: datetime format to be specified (optional)
    :return df: df with specified columns converted to datetime
    """
    try:
        if datetime_formats is None:
            datetime_formats = [None] * len(datetime_columns)

        for col, fmt in zip(datetime_columns, datetime_formats):
            df[col] = pd.to_datetime(df[col], format=fmt)
    except Exception as e:
        st.error(f"Error converting columns to date time - {e}")

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


def format_revenue_reporting(df: pd.DataFrame):
    """
    Add formatting changes to leaderboard table:
    - fill NA with 0s in BM
    - remove dps
    - add total column
    - report in £/MW/Yr
    - sort by highest £/MW/Yr
    :param df: revenue df
    :return: revenue df
    """
    clean_df = df.fillna(0)
    float_cols = ['DFR (£)', 'Wholesale MIDP (£)', 'Wholesale Sys (£)', 'BM (£)']
    clean_df[float_cols] = clean_df[float_cols].astype(int)

    clean_df['Total (£)'] = clean_df['DFR (£)'] + clean_df['Wholesale MIDP (£)'] + clean_df['BM (£)']
    clean_df['k/MW/yr'] = (clean_df['Total (£)'] * 365) / (clean_df['MW'] * 1000)
    clean_df = clean_df.sort_values(by='k/MW/yr', ascending=False)
    clean_df['k/MW/yr'] = clean_df['k/MW/yr'].astype(int)
    clean_df = clean_df.reset_index(drop=True)
    clean_df = clean_df.iloc[:, 2:]
    clean_df = clean_df.loc[:,
               ['Site', 'Owner', 'Optimiser', 'EFA Date', 'MW', 'MWh', 'DFR (£)', 'BM (£)', 'Wholesale MIDP (£)',
                'Wholesale Sys (£)', 'Total (£)', 'k/MW/yr']]

    return clean_df


def aggrid(df: pd.DataFrame):
    gb = GridOptionsBuilder.from_dataframe(df)

    gb.configure_auto_height(autoHeight=True)
    gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc="sum", editable=True)
    # gb.configure_grid_options(domLayout='normal')
    gb.configure_pagination(enabled=False, paginationAutoPageSize=False, paginationPageSize=12)
    gb.configure_first_column_as_index(headerText='Site')
    # sidebar stopped working after some other config changes mainly autosizing columns
    gb.configure_side_bar()
    gridOptions = gb.build()


    grid = AgGrid(
        df,
        width='100%',
        gridOptions=gridOptions,
        enable_enterprise_modules=False,
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS
    )

    return grid


def plot_revenue_daily(df: pd.DataFrame):
    """
    Plot total revenues
    :param df:
    :return fig:
    """
    copy = df.copy()
    copy['DFR'] = (copy['DFR (£)'] * 365) / (copy['MW'])
    copy['Wholesale'] = (copy['Wholesale MIDP (£)'] * 365) / (copy['MW'])
    copy['BM'] = (copy['BM (£)'] * 365) / (copy['MW'])
    fig = px.bar(copy, x='Site', y=['DFR', 'Wholesale', 'BM'],
                 labels={'variable': 'Category', 'value': '£/MW/yr'})
    return fig


@st.cache_data(show_spinner='loading...')
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')
