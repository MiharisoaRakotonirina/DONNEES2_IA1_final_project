import pandas as pd

def clean_city_name():
    path = "./data/star_schema/dim_ville.csv"
    df = pd.read_csv(path)

    df["city_name"] = df["city_name"].str.replace("Arrondissement de ", "", regex=False).str.strip()

    df["city_name"] = df["city_name"].str.title()

    df.to_csv(path, index=False)
    print("✅ City names cleaned and saved to ", path)

clean_city_name()