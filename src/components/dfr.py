import os
import sys
import pandas as pd
import requests
import concurrent.futures
import streamlit as st
from dotenv import load_dotenv

from src.logger import logger
from src.exception import CustomException


@st.cache_data(ttl=60, show_spinner="loading...")
def get_DFR_revenue(start: str, end: str, max_workers=12):
    """
    Get all detailed system prices from Modo API.
    :param start: start date
    :param end: end date
    :param max_workers:
    :return: BM df
    """
    url = f"https://api.modo.energy/public/v1/response_reform/results_by_unit?date_from={start}&date_to={end}"
    MODO_API_KEY = os.environ.get("MODO_API_KEY")
    df_list = []
    headers = {"X-Token": MODO_API_KEY}

    try:
        logger.info(
            f"Entered the Modo dynamic frequency function, getting dfr auction results for settlement date : {start}"
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            while url is not None:
                future = executor.submit(requests.get, url, headers=headers)
                response = future.result()
                response.raise_for_status()
                df_list.append(pd.DataFrame(response.json()["results"]))
                url = response.json()["next"]

        df = pd.concat(df_list)
        df_not_empty = len(df) > 0
        if df_not_empty:
            logger.info(
                "Successfully obtained detailed system prices from MODO 'response_reform' API endpoint"
            )
            # convert values to num where relevant
            df = df.apply(pd.to_numeric, errors="ignore")
            df.columns = [
                "Company",
                "Unit Name",
                "EFA Date",
                "Delivery Start",
                "Delivery End",
                "EFA",
                "Service",
                "Cleared Volume",
                "Clearing Price",
                "Technology Type",
                "Cancelled",
            ]
            subset = df.loc[
                :,
                [
                    "Delivery Start",
                    "Delivery End",
                    "Clearing Price",
                    "Service",
                    "Company",
                    "EFA Date",
                    "Unit Name",
                    "Cleared Volume",
                    "EFA",
                ],
            ]
            # multiply each row by 4 since the price and volume are reported per EFA block
            subset["Revenue"] = subset["Clearing Price"] * subset["Cleared Volume"] * 4

            return subset

        else:
            logger.info("Invalid request - user needs to choose a valid date")
            st.error(
                f"Error obtaining dynamic frequency response reform from Modo API - please enter a valid date"
            )

    except Exception as e:
        logger.error("Error obtaining DFR from Modo API")
        raise CustomException(e, sys)


def filter_dfr_bmu(map_df: pd.DataFrame, dfr_df: pd.DataFrame):
    """
    Search the dfr df retrieved from Grid for bmus which have been derived from
    the mapping xlsx 'asset_ids.xlsx'
    :param dfr_df: grid dfr auction results stored in df
    :param map_df: mapping file converted to df
    :return df: retrieves final df which contains all bmus and above 5MW
    """
    # Get a list of all unique values in the 'DFR/FFR ID' column of mapping file
    try:
        dfr_ffr_ids = map_df["DFR/FFR ID"].unique()

        # Create a boolean mask that is True for rows where the 'Unit Name' column contains
        # any value from the 'DFR/FFR ID' column of df1
        mask = dfr_df["Unit Name"].isin(dfr_ffr_ids)
        dfr_mask = dfr_df[mask]
        # find the dfr revenues for each unit
        dfr_sum_by_unit = (
            dfr_mask.groupby(["Unit Name", "EFA Date"])["Revenue"].sum().reset_index()
        )
        return dfr_sum_by_unit
    except Exception as e:
        st.error(f"Error finding BMUs in Grid's DFR dataset: {e}")


def get_bmu_dfr_dict(df: pd.DataFrame):
    """
    Iterate through the (bmu) asset ids mapping file and create a nested mapping dict
    :param df: mapping file
    :return dict: where outer key is the DFR unit ID and value is nested dict. inner dict keys are 'site,
    'owner', 'optimiser', 'MW' 'MWh' and values are actual site, owner, optimiser, bmu ids, MW and MWhs.
    """
    try:
        nested_dict = {}
        for index, row in df.iterrows():
            inner_dict = {
                "BMU ID": row["BMU ID"],
                "Site": row["Site"],
                "Owner": row["Owner"],
                "Optimiser": row["Optimiser"],
                "MW": row["MW"],
                "MWh": row["MWh"],
            }
            outer_key = row["DFR/FFR ID"]
            if outer_key not in nested_dict:
                nested_dict[outer_key] = {}
            nested_dict[outer_key] = inner_dict
        return nested_dict
    except Excepion as e:
        st.error(
            f"Error creating dict where key is DFR ID and value is owner, optimiser, MW and MWhs: {e}"
        )


def replace_units_with_site_names(df: pd.DataFrame, dict_map: dict):
    """
    Use the dict mapping to replace Unit IDs with actual site names
    :param dict_map: where key is the DFR unit ID and value is the Site name
    :param df: DFR auction results from Grid
    :return df: Site names replace Unit IDs
    """
    # create a list of site names if the bmu asset with DFR ID is present in the DFR df
    try:
        sites_list = []
        owner_list = []
        optimiser_list = []
        MW_list = []
        MWh_list = []
        bmus = []
        for unit in df["Unit Name"].values:
            for key in dict_map.keys():
                if unit == key:
                    sites_list.append(dict_map[key]["Site"])
                    owner_list.append(dict_map[key]["Owner"])
                    optimiser_list.append(dict_map[key]["Optimiser"])
                    MW_list.append(dict_map[key]["MW"])
                    MWh_list.append(dict_map[key]["MWh"])
                    bmus.append(dict_map[key]["BMU ID"])
        df["BMU ID"] = bmus
        df["Site"] = sites_list
        df["Owner"] = owner_list
        df["Optimiser"] = optimiser_list
        df["MW"] = MW_list
        df["MWh"] = MWh_list
        df = df.rename(columns={"Revenue": "DFR (£)"})
        # sort by highest DFR revenue
        # df = df.sort_values(by='DFR (£)', ascending=False).reset_index(drop=True)
        return df
    except Exception as e:
        st.error(f"Error when obtaining list of sites: {e}")


def append_wholesale_to_dfr(dfr_df: pd.DataFrame, wholesale_df: pd.DataFrame):
    """
    Combine wholesale MIDP df revenue with DFR df on common Unit Names
    :param dfr_df: DFR revenue df
    :param wholesale_df: Wholesale MIDP revenue df
    :return df: combined revenues DF per settlement date
    """
    combine_df = pd.merge(dfr_df, wholesale_df, on="Unit Name")
    combine_df = combine_df.fillna(0)

    # rearrange columns order
    combine_df = combine_df.loc[
        :,
        [
            "Unit Name",
            "BMU ID",
            "Site",
            "MW",
            "MWh",
            "EFA Date",
            "Owner",
            "Optimiser",
            "DFR (£)",
            "Wholesale MIDP (£)",
            "Wholesale Sys (£)",
        ],
    ]

    return combine_df
