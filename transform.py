import pandas as pd
from extract import extract

""" Defines the transform function to process the raw weather data extracted from the Open-Meteo API.
params:
    - df: A pandas DataFrame containing the raw weather data.
    - forecast_days: The number of future days to include in the forecast (default is 7).
returns:
    - A pandas DataFrame containing the transformed weather data.
"""
def transform(df, forecast_days=7):
    df = df.copy()

    # Ensure date column is in datetime format and sort by date (important for rolling features)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # Create new features based on existing weather data
    df["temp_range"] = df["temperature_2m_max"] - df["temperature_2m_min"]
    df["feels_like_diff"] = df["apparent_temperature_max"] - df["temperature_2m_max"]

    # Create categorical features based on temperature thresholds
    df["is_rainy"] = df["precipitation_sum"] > 0
    df["high_uv"] = df["uv_index_max"] > 7
    df["windy_day"] = df["wind_speed_10m_max"] > 15

    # Create a categorical feature for temperature ranges
    def temp_category(temp):
        if temp < 50:
            return "Cold"
        elif temp < 75:
            return "Moderate"
        else:
            return "Hot"
    df["temp_category"] = df["temperature_2m_max"].apply(temp_category)

    # Create time-based features
    df["day_of_week"] = df["date"].dt.day_name()
    df["month"] = df["date"].dt.month
    df["is_weekend"] = df["day_of_week"].isin(["Saturday", "Sunday"])

    # Create rolling features for temperature and precipitation
    df["temp_3day_avg"] = df["temperature_2m_max"].rolling(window=3).mean()
    df["precip_3day_sum"] = df["precipitation_sum"].rolling(window=3).sum()

    # Filter to include only forecast days
    df["is_forecast"] = df["date"] >= df["date"].max() - pd.Timedelta(days=forecast_days - 1)
    df = df[df["is_forecast"]]

    # Standardize column names to lowercase
    df.columns = df.columns.str.lower()
    
    return df