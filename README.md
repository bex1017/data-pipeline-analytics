# Rebecca Grandinette
Github URL: https://github.com/bex1017/data-pipeline-analytics.git
LinkedIn URL: www.linkedin.com/in/rebecca-grandinette-963112267


# End-to-End Weather Data Pipeline

An end-to-end data engineering project that extracts, transforms, stores, and analyzes weather data using Python and SQL. This project demonstrates real-world pipeline design and time-series analysis.


## Overview

This project builds a complete ETL (Extract, Transform, Load) pipeline using the Open-Meteo API. It retrieves both historical and forecast weather data, processes it into structured formats, stores it in a relational database, and generates analytical insights through SQL.


## Architecture

Open-Meteo API → Extract → Transform → Load (SQLite) → Analyze


## Features

- API integration with caching and retry logic  
- Configurable data extraction (historical + forecast support)  
- Data cleaning and preprocessing  
- Feature engineering and derived metrics  
- Time-series analysis with rolling averages  
- Relational database storage (SQLite)  
- SQL-based data analysis  
- Automated CSV output generation  


## Tech Stack

- **Languages:** Python, SQL  
- **Libraries:** pandas, sqlite3, requests-cache, retry-requests  
- **Data Source:** Open-Meteo API  


## Data Extraction

The pipeline retrieves daily weather data including:

- Temperature (max/min)
- Apparent temperature
- Precipitation
- UV index
- Wind speed


## Future Improvements

- Add data visualization utilizing an Excel or Power BI dashboard.
- Automate pipeline scheduling utilizing cron or Airflow
- Containerize with Docker


## Running the Pipeline

git clone https://github.com/bex1017/data-pipeline-analytics.git
cd data-pipeline-analytics

pip install -r requirements.txt

python main.py

