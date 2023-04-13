import streamlit as st
from src.utils import get_asset_ids
from src.components.wholesale import get_physical_notifications, get_MIDP, get_system_price, load_dotenv
import datetime
from src.logger import logger

# set revenue home page header to leaderboard and bar icon
# bring all modules into here
st.header("Revenue Leaderboard")

date = st.date_input(label='Select a date:', value=datetime.datetime.now().date())
# read in all asset ids into df
asset_ids_df = get_asset_ids()

# retrieve all physical notifications from PN stream endpoint from Elexon API
pns = get_physical_notifications(start=date, end=date, assets_df=asset_ids_df)

# retrive market index data price for chosen settlement date
midp = get_MIDP(start=date, end=date)

# retrieve system price for chosen settlement date
sysprice = get_system_price(start=date, end=date)
st.dataframe(midp, use_container_width=True)
