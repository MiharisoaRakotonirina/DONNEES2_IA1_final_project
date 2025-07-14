import pandas as pd
from datetime import datetime, timedelta
import os

def clean_historical_weather():
    input_path = './data/historical_weather_data/historical_weather_data.csv'
    output_path = './data/historical_weather_data/cleaned_historical_weather_data.csv'

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Load the historical weather data
    df = pd.read_csv(input_path)

    # Select needed columns and rename to get a file like fact_weather_cleaned.csv
    df = df[[
        "name",          # City name
        "datetime",      # Forecast date
        "temp",          # Average temperature
        "humidity",      # Humidity
        "windspeed",     # Wind speed
        "precipprob",    # Precipitation probability
        "description"    # Description of the weather
    ]].rename(columns={
        "name": "city_name",
        "datetime": "forecast_date",
        "windspeed": "wind_speed",
        "precipprob": "rain_prob"
    })

    # Clean : convert data types
    df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
    df["rain_prob"] = df["rain_prob"].fillna(0).astype(float).round(1)
    df["temp"] = df["temp"].astype(float).round(2)
    df["humidity"] = df["humidity"].astype(float).round(1)
    df["wind_speed"] = df["wind_speed"].astype(float).round(2)

    # Add new column (date_extraction)
    df["date_extraction"] = pd.to_datetime(df["forecast_date"]) - timedelta(days=1)
    df["date_extraction"] = df["date_extraction"].dt.date

    # city_id according to dim_ville.csv
    city_id_map = {
        "Paris": 1,
        "Lyon": 2,
        "Toulouse": 3,
        "Nice": 4,
        "Marseille": 5
    }

    # Add new column city_id according to city_id_map
    df["city_id"] = df["city_name"].map(city_id_map)

    # Check if all cities have a city_id
    if df["city_id"].isnull().any():
        missing = df[df["city_id"].isnull()]["city_name"].unique()
        raise ValueError(f"❌ Some cities don't have city_id : {missing}")
    
    # Reorder columns to keep the same order as in fact_weather_cleaned.csv
    df = df[[
        "forecast_date", "city_id", "temp", "humidity",
        "wind_speed", "rain_prob", "description", "date_extraction"
    ]]
    
    df.to_csv(output_path, index=False)
    print(f"✅ Historical weather data cleaned and saved to {output_path}")


clean_historical_weather()
