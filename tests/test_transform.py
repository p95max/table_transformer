import pandas as pd
from scripts.gs_transformer import prepare_features


def sample_df():

    data = [
        {
            "date": "2025-10-17",
            "Область": "RegionA",
            "Місто": "CityA",
            "Value 1": 2,
            "Value 2": 1,
            "Value 3": 0,
            "Value 4": 0,
            "Value 5": 0,
            "Value 6": 0,
            "Value 7": 0,
            "Value 8": 0,
            "Value 9": 0,
            "Value 10": 0,
            "long": 12.34,
            "lat": 56.78,
        }
    ]
    return pd.DataFrame(data)


def test_prepare_features_basic():
    df = sample_df()
    features = prepare_features(df)
    assert len(features) == 2
    first_attrs = features[0]["attributes"]
    second_attrs = features[1]["attributes"]
    assert first_attrs["i_value_1"] == 1
    assert first_attrs["i_value_2"] == 1
    assert second_attrs["i_value_1"] == 1
    assert second_attrs["i_value_2"] == 0
