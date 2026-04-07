import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

""" Defines the extract function to retrieve weather data from the Open-Meteo API.
params:
    - latitude: The latitude of the location for which to retrieve weather data (default is 40.4406 for Pittsburgh).
    - longitude: The longitude of the location for which to retrieve weather data (default is -79.9959 for Pittsburgh).
    - forecast_days: The number of future days to include in the forecast (default is 7).
    - past_days: The number of past days to include in the data (default is 0).
returns:
    - A pandas DataFrame containing the daily weather data for the specified location.
The function uses caching and retry mechanisms to handle API requests efficiently and robustly.
"""
def extract(latitude=40.4406, longitude=-79.9959, forecast_days=7, past_days=0):
    # Setup API client
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": [
            "temperature_2m_max", "temperature_2m_min",
            "apparent_temperature_max", "apparent_temperature_min",
            "uv_index_max", "precipitation_sum",
            "precipitation_probability_max", "wind_speed_10m_max"
        ],
        "past_days": past_days,
        "forecast_days": forecast_days,
        "timezone": "America/New_York",
        "wind_speed_unit": "mph",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()

    daily_data = {
        "date": pd.date_range(
            start=pd.to_datetime(daily.Time() + response.UtcOffsetSeconds(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd() + response.UtcOffsetSeconds(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        ),
        "temperature_2m_max": daily.Variables(0).ValuesAsNumpy(),
        "temperature_2m_min": daily.Variables(1).ValuesAsNumpy(),
        "apparent_temperature_max": daily.Variables(2).ValuesAsNumpy(),
        "apparent_temperature_min": daily.Variables(3).ValuesAsNumpy(),
        "uv_index_max": daily.Variables(4).ValuesAsNumpy(),
        "precipitation_sum": daily.Variables(5).ValuesAsNumpy(),
        "precipitation_probability_max": daily.Variables(6).ValuesAsNumpy(),
        "wind_speed_10m_max": daily.Variables(7).ValuesAsNumpy(),
    }

    df = pd.DataFrame(daily_data)

    return df