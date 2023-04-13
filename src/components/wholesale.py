import concurrent.futures
import sys
import os
import io

import pandas as pd
import requests
import streamlit as st

from src.exception import CustomException
from src.logger import logger
from src.utils import get_wavg
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

    url = f"https://data.elexon.co.uk/bmrs/api/v1/datasets/mid/stream?from={start}&to={end}&settlementPeriodFrom=1&settlementPeriodTo=48"

    try:
        logger.info(
            f"Entered the market index data price function, getting midp for settlement date: {start}"
        )
        with concurrent.futures.ThreadPoolExecutor(12) as executor:
            # Submit requests to the ThreadPoolExecutor
            future = executor.submit(requests.get, url)
            response = future.result()
            response.raise_for_status()
            js = response.json()
            df = pd.DataFrame(js)
            df_not_empty = len(df) > 0

            if df_not_empty:
                logger.info("Successfully obtained market index price from Elexon 'MID/Stream' API endpoint"
                             )
                df.columns = ['Dataset', 'Timestamp', 'provider', 'settlementDate', 'settlementPeriod', 'MIDP (£/MWh)',
                              'Volume']
                df = df.loc[:, ['provider', 'settlementDate', 'settlementPeriod', 'MIDP (£/MWh)', 'Volume']]
                df = df.apply(pd.to_numeric, errors='ignore')

                return df
            else:
                logger.info("Invalid request - user needs to choose a valid date")
                st.error(
                    f"Error obtaining market index price from Elexon API - please enter a valid date"
                )

    except Exception as e:
        logger.error("Error obtaining MIDP from Elexon 'MID/Stream'' API endpoint")
        raise CustomException(e, sys)


def clean_MIDP(df: pd.DataFrame, values: str, weights: str) -> pd.DataFrame:
    """
    The MIDP endpoint from Elexon retrieves market index price data across 2 providers, namely N2EX and APX. This
    function calculated the weighted average across these 2 providers where weights are the volume for each
    corresponding provider. The weighted average is grouped by settlement period and the final df contains the weighted
    average market index price per settlement period.
    :param df: Market Index Data price df
    :param values:
    :param weights:
    :return:
    """
    try:
        df = df.groupby("settlementPeriod").apply(get_wavg, values=values, weights=weights).reset_index()
        df.columns = ["settlementPeriod", "MIDP (£/MWh)"]
        return df
    except Exception as e:
        st.error(f"Error when finding wighted average of MIDP: {e}")


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
        logger.error("Error obtaining system price from Elexon 'DERSYSDATA' API endpoint")
        raise CustomException(e, sys)


def split_pns_df(df: pd.DataFrame, mid_price_df: pd.DataFrame, sys_price_df: pd.DataFrame):
    """
    Split PNs into 'Equal' and 'Not Equal'. When the 'levelFrom' and 'levelTo' columns are equal, the MIDP is multiplied
    by the 'levelFrom' volume for Wholesale revenue calculation. This is marked by the variable 1 in the 'Equal' column.
    When the 'levelFrom' and 'levelTo' columns are not equal for a BMU in a settlement period then that implies the
    volume pn-ed has changed during the half hour settlement period. To calculate the subsequent wholesale revenue,
    the net pn-ed volume is calculated in a separate column 'Net PN' and this throughput is multiplied by MIDP to
    calculate revenue.
    # FIXME add docstrings
    :param sys_price_df:
    :param mid_price_df:
    :param df:
    :return:
    """
    # check the PNs df to see where levelfrom and levelto are equal

    try:

        # merge the midp onto the pns so now each row has a unique price per sp
        df = pd.merge(df, mid_price_df, on='settlementPeriod')
        # then merge sys price onto same df
        df = pd.merge(df, sys_price_df, on='settlementPeriod')

        df = df.fillna(0)

        # find timedelta
        df['Hours'] = (df['timeTo'] - df['timeFrom']).dt.seconds / 3600
        df['Equal'] = np.where(df['levelFrom'] == df['levelTo'], 1, 0)

        not_eq_df = df[df['Equal'] == 0]
        eq_df = df[df['Equal'] == 1]

        # if levels aren't equal then sum the levels to get net throuhput
        not_eq_df['Net PN'] = not_eq_df['levelFrom'] + not_eq_df['levelTo']

        return eq_df, not_eq_df
    except Exception as e:
        st.error(f'Error obtaining final FPNs: {e}')
