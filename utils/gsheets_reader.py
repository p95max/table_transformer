"""
Helper to read a Google Sheet into pandas.DataFrame using a service account JSON.
"""

from typing import Optional
import pandas as pd
import gspread


def read_sheet_to_df(service_account_json: str, sheet_id: str, worksheet_name: str = "Sheet1") -> pd.DataFrame:
    """
    Read Google Worksheet into pandas DataFrame.
    """
    gc = gspread.service_account(filename=service_account_json)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)
    rows = ws.get_all_records()
    return pd.DataFrame(rows)
