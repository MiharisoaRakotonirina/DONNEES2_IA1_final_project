import pandas as pd
import os

def transform_to_star_schema() -> str:
    # 1. Path
    input_file = "./data/processed/meteo_global.csv"
    output_dir = "./data/star_schema"
    os.makedirs(output_dir, exist_ok=True)

    # 2. Load data
    meteo_df = pd.read_csv(input_file)

    meteo_df["city_name"] = meteo_df["city_name"].str.replace("Arrondissement de ", "", regex=False).str.strip()
    meteo_df["city_name"] = meteo_df["city_name"].str.title()

    # 3. clean columns, keep only essentials
    meteo_df = meteo_df[[
        "datetime", "temp", "humidity", "wind_speed", "rain_prob",
        "description", "city_name", "city_country", "date_extraction"
    ]]

    meteo_df["rain_prob"] = meteo_df["rain_prob"].round(1)

    # 4. create city dimension (primary key: city_id)
    dim_ville_path = f"{output_dir}/dim_ville.csv"

    if os.path.exists(dim_ville_path):
        dim_ville = pd.read_csv(dim_ville_path)
    else:
        dim_ville = pd.DataFrame(columns=["city_id", "city_name", "city_country"])

    existing_cities = set(zip(dim_ville["city_name"], dim_ville["city_country"]))
    new_cities = set(zip(meteo_df["city_name"], meteo_df["city_country"])) - existing_cities

    if new_cities:
        next_id = int(dim_ville["city_id"].max()) + 1 if not dim_ville.empty else 1
        new_rows = pd.DataFrame({
            "city_id": range(next_id, next_id + len(new_cities)),
            "city_name": [v[0] for v in new_cities],
            "city_country": [v[1] for v in new_cities]
        })
        dim_ville = pd.concat([dim_ville, new_rows], ignore_index=True)
        dim_ville.to_csv(dim_ville_path, index=False)

    # 5. join cities (foreign key in fact table)
    facts_df = meteo_df.merge(dim_ville, on=["city_name", "city_country"], how="left")

    if "city_id" not in facts_df.columns:
        raise ValueError("❌ ERROR: 'city_id' missing after join — check dim_city.csv")

    if facts_df["city_id"].isnull().any():
        missing = facts_df[facts_df["city_id"].isnull()][["city_name", "city_country"]].drop_duplicates()
        raise ValueError(f"❌ Some cities don't have a 'city_id':\n{missing}")


    # 7. Create fact table
    facts_df = facts_df[[
        "datetime", "temp", "humidity", "wind_speed", "rain_prob",
        "description", "city_id", "date_extraction"
    ]]

    # 8. Save
    fact_path = f"{output_dir}/fact_weather.csv"
    facts_df.to_csv(fact_path, index=False)

    print("✅ Generate star schema ")
    return fact_path

#transform_to_star_schema_with_score()