import os
import sys
import pandas as pd
import requests
import concurrent.futures
import streamlit as st
from dotenv import load_dotenv

from src.logger import logger
from src.exception import CustomException

# Load environment variables from .env file
load_dotenv()


@st.cache_data(ttl=60, show_spinner="loading...")
def get_BM_revenue(start: str, end: str, max_workers=12):
    """
    Get all detailed system prices from Modo API.
    :param start: start date
    :param end: end date
    :param max_workers:
    :return: BM df
    """
    url = f"https://api.modo.energy/public/v1/detail_system_price?date_from={start}&date_to={end}"
    MODO_API_KEY = os.environ.get("MODO_API_KEY")
    df_list = []
    headers = {"X-Token": MODO_API_KEY}

    try:
        logger.info(
            f"Entered the Modo detailed system prices function, getting DETSYS for settlement date : {start}"
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
                "Successfully obtained detailed system prices from MODO 'DETSYS' API endpoint"
            )
            df = df.reset_index(drop=True)

            subset = df.loc[
                :,
                [
                    "date",
                    "period",
                    "record_type",
                    "price",
                    "volume",
                    "sys_price_id",
                    "so_flag",
                ],
            ]
            subset.columns = [
                "date",
                "period",
                "record_type",
                "price",
                "volume",
                "BMU ID",
                "so_flag",
            ]
            subset["BM (£)"] = subset["price"] * subset["volume"]
            subset = subset.groupby("BMU ID")["BM (£)"].sum().reset_index()

            return subset

        else:
            logger.info("Invalid request - user needs to choose a valid date")
            st.error(
                f"Error obtaining DETSYS from Modo API - please enter a valid date"
            )

    except Exception as e:
        logger.error("Error obtaining DETSYS from Modo API")
        raise CustomException(e, sys)
