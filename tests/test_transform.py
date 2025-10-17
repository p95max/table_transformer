"""
Тести для scripts/transform_to_postgis.py - prepare_features_from_df,
обробка координат/дати, поведінка за відсутності колонок або за нульових значень.
"""
import pandas as pd
import datetime
import pytest
from scripts.transform_to_postgis import (
    prepare_features_from_df,
    normalize_number_str,
)


def make_row(date, region, city, values, lon, lat):
    row = {
        "date": date,
        "Region": region,
        "City": city,
        "long": lon,
        "lat": lat,
    }
    # add Value 1..10
    for i in range(1, 11):
        row[f"Value {i}"] = values.get(i, 0)
    return row


def test_normalize_number_str_comma():
    assert normalize_number_str("12,34") == 12.34
    assert normalize_number_str("  56,78 ") == 56.78
    assert normalize_number_str("") is None
    assert normalize_number_str(None) is None
    assert normalize_number_str("100") == 100


def test_prepare_features_basic():
    rows = [
        make_row("17.10.2025", "RegionA", "CityA", {1: 2, 2: 1}, "12,34", "56,78"),
        make_row("18/10/2025", "RegionB", "CityB", {1: 0, 2: 0}, "13.00", "57.00"),  # all zeros -> skipped
    ]

    ordered_cols = ["date", "Region", "City"] + [f"Value {i}" for i in range(1, 11)] + ["long", "lat"]

    df = pd.DataFrame(rows)
    df = df.reindex(columns=ordered_cols)

    features, preview_rows, meta = prepare_features_from_df(df)
    assert len(features) == 2
    assert len(preview_rows) == 2
    first_attrs = features[0]["attributes"]
    assert first_attrs["t_region"] == "RegionA"
    assert first_attrs["t_city"] == "CityA"
    assert "i_value_1" in first_attrs and "i_value_10" in first_attrs
    assert isinstance(first_attrs["long"], float)
    assert isinstance(first_attrs["lat"], float)

    assert "value_cols" in meta


def test_prepare_features_date_formats_and_string_if_invalid():
    df = pd.DataFrame([
        {
            "Дата": "2025-10-17",
            "Область": "R",
            "Місто": "C",
            **{f"Значення {i}": (1 if i == 1 else 0) for i in range(1, 11)},
            "long": "12,34",
            "lat": "56,78",
        },
        {
            "Дата": "not-a-date",
            "Область": "R2",
            "Місто": "C2",
            **{f"Значення {i}": 0 for i in range(1, 11)},
            "long": "12.0",
            "lat": "56.0",
        },
    ])
    features, preview, meta = prepare_features_from_df(df)
    assert len(features) == 1
    attrs = features[0]["attributes"]
    assert attrs["d_date"] == "2025-10-17" or attrs["d_date"].startswith("2025")
    assert all(row["t_region"] != "R2" for row in preview)


def test_prepare_features_missing_value_cols_raises():
    df = pd.DataFrame([{"date": "2025-10-17", "Region": "R", "City": "C", "long": "1", "lat": "2"}])
    with pytest.raises(RuntimeError):
        prepare_features_from_df(df)
