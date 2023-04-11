import concurrent.futures
import sys
import os
import io

import pandas as pd
import requests
import streamlit as st

from src.exception import CustomException
from src.logger import logger
from dotenv import load_dotenv
from ElexonDataPortal import api
import datetime

# Load environment variables from .env file
load_dotenv()


@st.cache_data(ttl=60, show_spinner="loading...")
def get_physical_notifications(
        start: str, end: str, assets_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Get fpn data for fleet from ELEXON API.
    :param assets_df: df containing asset ids
    :param start: Date from in request (YY-m-d)
    :param end: Date to in request (YY-m-d)
    :return df: pd.DataFrame
    """

    try:
        logger.info(
            "Entered the physical notifications (pn) function, getting pns for all unique bmus"
        )
        unique_bmus = assets_df["BMU ID"].unique().tolist()

        params = {"bmUnit": unique_bmus}
        url = (
            f"https://data.elexon.co.uk/bmrs/api/v1/datasets/PN/stream?from={start}&to={end}&settlementPeriodFrom=1"
            f"&settlementPeriodTo=48 "
        )
        with concurrent.futures.ThreadPoolExecutor(12) as executor:
            # Submit requests to the ThreadPoolExecutor
            future = executor.submit(requests.get, url, params=params)
            response = future.result()
            response.raise_for_status()
            df = pd.DataFrame(response.json())
            df_not_empty = len(df) > 0

            if df_not_empty:
                logger.info(
                    "Successfully obtained pns from Elexon 'PN stream' API endpoint"
                )

                return df

            else:
                logger.info("Invalid request - user needs to choose a valid date")
                st.error(
                    f"Error obtaining PNs from Elexon API - please enter a valid date"
                )

    except Exception as e:
        logger.error("Error obtaining pns from Elexon 'PN stream' API endpoint")
        raise CustomException(e, sys)


def get_MIDP(start: str, end: str) -> pd.DataFrame:
    """
    Get Market Index Price data for each sp from ELEXON API.
    :param start: start date inputted by user in st.date_input
    :param end: end date inputted by user in st.date_input
    :return: pd.DataFrame
    """
    # Get API key from environment variable
    APIKey = os.environ.get("ELEXON_API_KEY")
    client = api.Client(APIKey)

    try:
        # FIXME error when calling get_MID
        mid = client.get_MID(start_date=start, end_date=end)
        mid.columns = ['datetime', 'record', 'provider', 'settlementDate', 'settlementPeriod', 'MIDP (£/MWh)',
                       'Volume', 'Flag']
        mid_subset = mid.loc[:, ['provider', 'settlementDate', 'settlementPeriod', 'MIDP (£/MWh)', 'Volume']]
        mid_subset = mid_subset.apply(pd.to_numeric, errors='ignore')
        return mid_subset
    except Exception as e:
        st.error(f'Error retrieving MIDP from Elexon: {e}')


@st.cache_data(ttl=60, show_spinner='loading...')
def get_system_price(start: str, end: str) -> pd.DataFrame:
    """
    Get System Price data for each sp from ELEXON API.
    :param start: Date from in request (YY-m-d)
    :param end: Date to in request (YY-m-d)
    :return pd.DataFrame: columns -> settlementPeriod, Sys Price (£/MWh)
    """
    APIKey = os.environ.get("ELEXON_API_KEY")

    try:
        url = f"https://api.bmreports.com/BMRS/DERSYSDATA/v1?APIKey={APIKey}&FromSettlementDate={start}&ToSettlementDate={end}&ServiceType=csv"
        r = requests.get(url)
        r.raise_for_status()
        urlData = r.content

        rawData = pd.read_csv(io.StringIO(urlData.decode('utf-8')))
        rawData = rawData.reset_index()
        df_not_empty = len(rawData) > 0

        if df_not_empty:
            logger.info(
                "Successfully obtained system price from Elexon 'DERSYSDATA' API endpoint"
            )
            # clean sys price
            df = rawData.loc[:, ['level_2', 'level_3']]
            df.columns = ['settlementPeriod', 'Sys Price (£/MWh)']
            df = df.dropna()

            return df

        else:
            logger.info("Invalid request - user needs to choose a valid date")
            st.error(
                f"Error obtaining system price from Elexon API - please enter a valid date"
            )

    except Exception as e:
        logger.error("Error obtaining pns from Elexon 'DERSYSDATA' API endpoint")
        raise CustomException(e, sys)
