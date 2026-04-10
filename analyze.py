import sqlite3
import pandas as pd
import os

""" Defines functions to perform analysis on the weather data stored in the SQLite database and save results to CSV files.

get_connection function:
    params:
        - db_name: The name of the SQLite database file (default is "weather.db").
    returns:
        - A SQLite connection object.

query_data function:
    params:
        - query: A SQL query string to execute on the database.
        - conn: A SQLite connection object.
    returns:
        - A pandas DataFrame containing the results of the query.

ensure_output_dir function:
    params:
        - None
    returns:
        - None (the function creates an output directory if it doesn't exist).

analyze function:
    params:
        - db_name: The name of the SQLite database file (default is "weather.db").
    returns:
        - None (the function performs analysis and saves results to CSV files).
"""

OUTPUT_DIR = "outputs"


def get_connection(db_name="weather.db"):
    return sqlite3.connect(db_name)


def query_data(query, conn):
    return pd.read_sql_query(query, conn)


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def analyze(db_name="weather.db"):
    conn = get_connection(db_name)

    # Ensure output directory exists
    ensure_output_dir()

    print("\n=== Weather Data Analysis ===\n")

    # 1. Average temperature by day of week
    query1 = """
    SELECT day_of_week, AVG(temperature_2m_max) AS avg_temp
    FROM weather_data
    WHERE is_forecast = 1
    GROUP BY day_of_week
    ORDER BY avg_temp DESC;
    """
    df1 = query_data(query1, conn)
    print("Average Forecast Temperature by Day of Week:")
    print(df1, "\n")
    df1.to_csv(f"{OUTPUT_DIR}/avg_temp_by_day.csv", index=False)

    # 2. Hottest forecast days
    query2 = """
    SELECT date, temperature_2m_max
    FROM weather_data
    WHERE is_forecast = 1
    ORDER BY temperature_2m_max DESC
    LIMIT 5;
    """
    df2 = query_data(query2, conn)
    print("Top 5 Hottest Forecast Days:")
    print(df2, "\n")
    df2.to_csv(f"{OUTPUT_DIR}/hottest_days.csv", index=False)

    # 3. Rainy forecast days
    query3 = """
    SELECT date, precipitation_sum
    FROM weather_data
    WHERE is_forecast = 1 AND is_rainy = 1
    ORDER BY precipitation_sum DESC;
    """
    df3 = query_data(query3, conn)
    print("Rainy Forecast Days:")
    print(df3, "\n")
    df3.to_csv(f"{OUTPUT_DIR}/rainy_days.csv", index=False)

    # 4. Temperature vs rolling average
    query4 = """
    SELECT date, temperature_2m_max, temp_3day_avg
    FROM weather_data
    WHERE is_forecast = 1;
    """
    df4 = query_data(query4, conn)

    df4["temp_diff"] = df4["temperature_2m_max"] - df4["temp_3day_avg"]

    print("Temperature vs Rolling Average:")
    print(df4, "\n")
    df4.to_csv(f"{OUTPUT_DIR}/temp_vs_rolling.csv", index=False)

    # 5. Summary statistics
    query5 = """
    SELECT 
        AVG(temperature_2m_max) AS avg_temp,
        MAX(temperature_2m_max) AS max_temp,
        MIN(temperature_2m_min) AS min_temp,
        AVG(precipitation_sum) AS avg_precip
    FROM weather_data
    WHERE is_forecast = 1;
    """
    df5 = query_data(query5, conn)
    print("Summary Statistics:")
    print(df5, "\n")
    df5.to_csv(f"{OUTPUT_DIR}/summary_stats.csv", index=False)

    conn.close()

    print(f"Analysis complete. Results saved to '{OUTPUT_DIR}/' folder.")