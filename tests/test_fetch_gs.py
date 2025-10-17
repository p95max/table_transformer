"""
перевіряє utils.gsheets_reader.read_sheet_to_df - мокає gspread.service_account
"""
import pandas as pd
from types import SimpleNamespace
import pytest

import utils.gsheets_reader as gs_reader

class FakeWorksheet:
    def __init__(self, records):
        self._records = records
    def get_all_records(self):
        return self._records

class FakeSpreadsheet:
    def __init__(self, worksheets):
        self._worksheets = worksheets
    def worksheet(self, name):
        return self._worksheets[0]

def test_read_sheet_to_df(monkeypatch):
    fake_records = [
        {"col1": "a", "col2": 1},
        {"col1": "b", "col2": 2},
    ]
    fake_ws = FakeWorksheet(fake_records)
    fake_sh = FakeSpreadsheet([fake_ws])

    def fake_service_account(filename):
        return SimpleNamespace(open_by_key=lambda key: fake_sh)

    import gspread
    monkeypatch.setattr(gspread, "service_account", fake_service_account)

    df = gs_reader.read_sheet_to_df("dummy.json", "SHEET_ID", worksheet_name="Sheet1")
    assert isinstance(df, pd.DataFrame)
    assert df.shape[0] == 2
    assert list(df.columns) == ["col1", "col2"]
    assert df.iloc[0]["col1"] == "a"
