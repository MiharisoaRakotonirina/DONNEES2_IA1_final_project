import pandas as pd

def clean_weather_fact():
    path = "./data/star_schema/fact_weather.csv"
    df = pd.read_csv(path)


    # Extract date (within hour) for regrouping per day
    df["date"] = pd.to_datetime(df["datetime"]).dt.date

    aggregated = df.groupby(["date", "city_id"]).agg({
        "temp": "mean",
        "humidity": "mean",
        "wind_speed": "mean",
        "rain_prob": "mean",
        "description": lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0],
        "date_extraction" : "max"
    }).reset_index()

    # Rename column date to forecast_date for clarity
    aggregated.rename(columns={"date" : "forecast_date"}, inplace=True)

    # Round for more readable values
    aggregated["temp"] = aggregated["temp"].round(2)
    aggregated["humidity"] = aggregated["humidity"].round(1)
    aggregated["wind_speed"] = aggregated["wind_speed"].round(2)
    aggregated["rain_prob"] = aggregated["rain_prob"].round(1)

    output_path = "./data/star_schema/fact_weather_cleaned.csv"
    aggregated.to_csv(output_path, index=False)

    print("✅ Data cleaned and saved to", output_path)
    return output_path


#clean_weather_fact()