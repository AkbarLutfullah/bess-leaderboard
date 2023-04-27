# [BESS Leaderboard](https://bess-revenue-leaderboard.streamlit.app/)
A webapp that uses publicly available UK energy markets, contracted volume, and price data and estimates the daily revenue made by the energised UK Battery Energy Storage Systems (BESS) Fleet.<br>

<p align="left">
  <img src="https://user-images.githubusercontent.com/60239161/234632827-e5024c7b-1449-4338-8111-647f81c20f91.png" alt="bess leaderboard 0">
</p>

Revenue is reported on a near realtime, annualised basis (£/MW/yr) across the 3 primary revenue streams, namely Wholesale, Balancing Mechanism (BM), and Dynamic Frequency Response<br>

<p align="left">
  <img src="https://user-images.githubusercontent.com/60239161/234806816-47612f9a-3424-460a-89a0-434549c15d99.png" alt="bess leaderboard 1">
</p>

<p align="left">
  <img src="https://user-images.githubusercontent.com/60239161/234807295-7fc4b0a3-6ee8-41c9-b7d9-e396e477faa8.png" alt="bess leaderboard 2">
</p>

Historic daily revenues can be displayed using the date input widget<br>

<p align="left">
  <img src="https://user-images.githubusercontent.com/60239161/234807718-e9473319-73be-4d2b-b62d-0c59c5ce43e5.png" alt="bess leaderboard 4">
</p>

# Data sources
There are 2 main data sources:

1. [Elexon Limited - includes balancing mechanism physical notifications, bm bid and offer levels and acceptances, and system and market index (Epex/Nordpool) energy prices](https://bmrs.elexon.co.uk/api-documentation).

2. [National Grid ESO - includes dynamic frequency response auction results](https://data.nationalgrideso.com/data-groups/ancillary-services).

Check out the [app](https://bess-revenue-leaderboard.streamlit.app/) here.
