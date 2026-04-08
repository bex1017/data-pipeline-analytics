import sqlite3

""" Defines functions to create SQLite database and load weather data into it.

create_connection function:
    params:
        - db_name: The name of the SQLite database file (default is "weather.db").
    returns:
        - A connection object to the SQLite database.

create_table function:
    params:
        - conn: A connection object to the SQLite database.
    returns:
        - None (the function creates a table in the database if it doesn't exist).

normalize_dataframe function:
    params:
        - df: A pandas DataFrame containing the transformed weather data.
    returns:
        - A pandas DataFrame with normalized data types.

load function:
    params:
        - df: A pandas DataFrame containing the transformed weather data.
        - db_name: The name of the SQLite database file (default is "weather.db").
    returns:
        - None (the function creates a database file and loads data into it).
"""

def create_connection(db_name="weather.db"):
    return sqlite3.connect(db_name)


def create_table(conn):
    query = """
    CREATE TABLE IF NOT EXISTS weather_data (
        date TEXT PRIMARY KEY,
        temperature_2m_max REAL,
        temperature_2m_min REAL,
        apparent_temperature_max REAL,
        apparent_temperature_min REAL,
        uv_index_max REAL,
        precipitation_sum REAL,
        precipitation_probability_max REAL,
        wind_speed_10m_max REAL,
        temp_range REAL,
        feels_like_diff REAL,
        is_rainy INTEGER,
        high_uv INTEGER,
        windy_day INTEGER,
        temp_category TEXT,
        day_of_week TEXT,
        month INTEGER,
        is_weekend INTEGER,
        temp_3day_avg REAL,
        precip_3day_sum REAL,
        is_forecast INTEGER
    );
    """
    conn.execute(query)
    conn.commit()

def normalize_dataframe(df):
    df = df.copy()

    # Convert datetime to string
    df["date"] = df["date"].astype(str)

    # Convert boolean columns to int
    bool_cols = ["is_rainy", "high_uv", "windy_day", "is_weekend", "is_forecast"]
    for col in bool_cols:
        df[col] = df[col].astype(int)

    # Convert all NumPy types to native Python types
    df = df.apply(lambda col: col.map(lambda x: x.item() if hasattr(x, "item") else x))

    return df

def load(df, db_name="weather.db"):
    conn = create_connection(db_name)
    create_table(conn)

    df = normalize_dataframe(df)

    cursor = conn.cursor()

    # Ensure column order matches table
    columns = [
        "date",
        "temperature_2m_max",
        "temperature_2m_min",
        "apparent_temperature_max",
        "apparent_temperature_min",
        "uv_index_max",
        "precipitation_sum",
        "precipitation_probability_max",
        "wind_speed_10m_max",
        "temp_range",
        "feels_like_diff",
        "is_rainy",
        "high_uv",
        "windy_day",
        "temp_category",
        "day_of_week",
        "month",
        "is_weekend",
        "temp_3day_avg",
        "precip_3day_sum",
        "is_forecast"
    ]

    placeholders = ",".join(["?"] * len(columns))

    query = f"""
    INSERT OR REPLACE INTO weather_data ({",".join(columns)})
    VALUES ({placeholders})
    """

    for _, row in df.iterrows():
        cursor.execute(query, tuple(row[col] for col in columns))

    conn.commit()
    conn.close()