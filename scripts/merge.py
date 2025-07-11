import pandas as pd
import os
from datetime import datetime

RAW_DIR = "./data/raw"
PROCESSED_FILE = "./data/processed/meteo_global.csv"
os.makedirs(os.path.dirname(PROCESSED_FILE), exist_ok=True)

def merge_all_csv():
    # Extract date 
    extraction_date = datetime.today().strftime("%Y-%m-%d")

    # Read all CSV files
    all_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]
    df_list = []

    for file in all_files:
        file_path = os.path.join(RAW_DIR, file)
        df = pd.read_csv(file_path)

        # Ajout de la date d'extraction
        df["date_extraction"] = extraction_date
        df_list.append(df)

    if not df_list:
        raise ValueError("❌ Any CSV file found inside data/raw")

    # Merge all actual file
    new_data = pd.concat(df_list, ignore_index=True)

    # Load previous files if exist
    if os.path.exists(PROCESSED_FILE):
        global_df = pd.read_csv(PROCESSED_FILE)
    else:
        global_df = pd.DataFrame()

    # Merge all (old , new)
    combined_df = pd.concat([global_df, new_data], ignore_index=True)

    # (Same datetime + city_name + extract_date) Remove duplicates
    combined_df = combined_df.drop_duplicates(
        subset=["datetime", "city_name", "date_extraction"],
        keep="last"
    )

    #  Save merged file
    combined_df.to_csv(PROCESSED_FILE, index=False)
    print("✅ Merged successfully, Data saved inside data/processed/meteo_global.csv")


#merge_all_csv()