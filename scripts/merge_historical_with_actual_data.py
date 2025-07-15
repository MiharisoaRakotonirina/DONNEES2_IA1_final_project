import pandas as pd
import os

def merge_historical_with_actual_data():
    historical_path = './data/historical_weather_data/cleaned_historical_weather_data.csv'
    actual_path = './data/star_schema/fact_weather_cleaned.csv'
    output_path = './data/final/merged_weather_data.csv'

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # 2. Read historical & actual data files
    historical_df = pd.read_csv(historical_path)
    actual_df = pd.read_csv(actual_path)

    # 3. Check if their columns match
    if not historical_df.columns.equals(actual_df.columns):
        raise ValueError("❌ Columns of historical and actual data do not match.")

    # 4. Combine 
    merged_df = pd.concat([historical_df, actual_df], ignore_index=True)

    # 5. Drop duplicates (based on forecast_date + city_id)
    merged_df.drop_duplicates(subset=["forecast_date", "city_id"], keep="last", inplace=True)

    # 6. Save the merged file
    merged_df.to_csv(output_path, index=False)
    print(f"✅ Data successfully merged, saved inside {output_path}")

#merge_historical_with_actual_data()