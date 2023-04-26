import streamlit as st
import pandas as pd
from src.utils import get_asset_ids, convert_columns_to_datetime, format_revenue_reporting, aggrid, plot_revenue_daily, \
    convert_df
from src.components.wholesale import get_physical_notifications, split_pns_df, get_MIDP, clean_MIDP, get_system_price, \
    get_wholesale_revenue
from src.components.balancing_mechanism import get_BM_revenue
from src.components.dfr import get_DFR_revenue, filter_dfr_bmu, get_bmu_dfr_dict, replace_units_with_site_names, \
    append_wholesale_to_dfr
import streamlit_toggle as tog
import plotly.graph_objects as go
import datetime
from src.logger import logger

# setup page config
st.set_page_config(page_title="BESS Revenue", layout="wide", page_icon="📊", initial_sidebar_state='auto', )

# setup sidebar
st.sidebar.title("🔋 UK BESS Revenue")
st.sidebar.caption("Performance of utility scale Lithium-ion batteries in the UK.")
st.sidebar.markdown("Made by [Akbar Lutfullah](https://www.linkedin.com/in/akbar-lutfullah-b04881136/)")
st.sidebar.markdown("---")
st.sidebar.info("""##### The dashboard displays UK BESS (BM registered) energised assets with a nameplate capacity in excess of 5MW.

##### Wholesale revenues are estimated using physical notifications (contracted volumes), and market index price. 

##### Wholesale revenues using the system price are included in the dashboard but excluded in the total and annualised columns.

##### BM revenues are calculated by multiplying the contracted bid-offer volumes with the accepted bid-offer prices.

##### DFR revenues are the contracted auction fees.
""")


# set revenue home page header to leaderboard and bar icon
# bring all modules into here
st.title("Live BESS Fleet Dashboard :moneybag:")

date = st.date_input(label='Select a date:', value=datetime.datetime.now().date())
# read in all asset ids into df
asset_ids_df = get_asset_ids()

# retrieve all physical notifications from PN stream endpoint from Elexon API
pns = get_physical_notifications(start=date, end=date, assets_df=asset_ids_df)
cleaned_pns = convert_columns_to_datetime(df=pns, datetime_columns=['timeFrom', 'timeTo', 'settlementDate'],
                                          datetime_formats=[None, None, "%Y-%m-%d"])

# retrieve market index data price for chosen settlement date from Elexon API
midp = get_MIDP(start=date, end=date)
clean_midp = clean_MIDP(df=midp, values='MIDP (£/MWh)', weights='Volume')

# retrieve system price for chosen settlement date
sys_price = get_system_price(start=date, end=date)

# split physical notifications based on condition specified in function docstring
eq_df, not_eq_df = split_pns_df(df=cleaned_pns, mid_price_df=clean_midp, sys_price_df=sys_price)

# wholesale revenues inferred
wholesale_df, grouped_by_bmu_wholesale_df = get_wholesale_revenue(eq_df=eq_df, not_eq_df=not_eq_df)

# bm revenues inferred
grouped_by_bmu_bm_df = get_BM_revenue(start=date, end=date)

# retrieve all dfr auction results from Modo/Grid API
dfr_df = get_DFR_revenue(start=date, end=date)

dfr_bmu_df = filter_dfr_bmu(map_df=asset_ids_df, dfr_df=dfr_df)
bmu_dict = get_bmu_dfr_dict(df=asset_ids_df)
grouped_by_unit_dfr_df = replace_units_with_site_names(df=dfr_bmu_df, dict_map=bmu_dict)

# merge DFR revenues with Wholesale MIDP revenues
revenue_df = append_wholesale_to_dfr(dfr_df=grouped_by_unit_dfr_df, wholesale_df=grouped_by_bmu_wholesale_df)
revenue_df = revenue_df.loc[:,
             ['Unit Name', 'BMU ID', 'Site', 'MW', 'MWh', 'EFA Date', 'Owner', 'Optimiser', 'DFR (£)',
              'Wholesale MIDP (£)', 'Wholesale Sys (£)']]

df_dfr_ws_bm = pd.merge(revenue_df, grouped_by_bmu_bm_df, on='BMU ID', how='left')

format_df = format_revenue_reporting(df=df_dfr_ws_bm)
csv = convert_df(df=format_df)
timestamp = datetime.datetime.now()
st.sidebar.download_button(
    label="Download data as csv",
    data=csv,
    key='leaderboard',
    file_name=f'UK_Bess_Revenue_{timestamp}.csv',
    mime='text/csv',
)
st.sidebar.caption("""You can check out the source code [here](https://github.com/AkbarLutfullah/bess-leaderboard).""")
with st.container():
    st.success(f"Showing {len(format_df)} BMUs for {date}")
    chart = st.checkbox(label='Show revenue chart', value=False,
                        help='Display asset revenues in £/MW/yr from left to right')
    switch = tog.st_toggle_switch(label="Mobile View",
                                  key="Key1",
                                  default_value=False,
                                  label_after=False,
                                  inactive_color='#D3D3D3',
                                  active_color="#11567f",
                                  track_color="#29B5E8"
                                  )
    if switch:
        if chart:
            rev_plot = plot_revenue_daily(df=format_df)
            st.plotly_chart(rev_plot, use_container_width=True, config={'displaylogo': False})
            st.table(format_df)
        else:
            st.table(format_df)
    else:
        if chart:
            rev_plot = plot_revenue_daily(df=format_df)
            st.plotly_chart(rev_plot, use_container_width=True, config={'displaylogo': False})
            aggrid(df=format_df)
        else:
            aggrid(df=format_df)
