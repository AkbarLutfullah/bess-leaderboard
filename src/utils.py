import os
import pandas as pd


def get_asset_ids():
    """
    Retrieve xlsx containing all asset IDS for BM and DFR
    :return df: columns:
                site, owner, optimiser, bmu id, dfr/ffr id, mw, mwh
    """
    file_path = os.path.join(os.path.dirname(__file__), "..", "data", "asset_ids.xlsx")
    df = pd.read_excel(file_path, sheet_name="bess bm")

    return df
