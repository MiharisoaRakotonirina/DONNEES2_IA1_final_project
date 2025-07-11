import pandas as pd
import os

def transform_to_star_schema_with_score() -> str:
    """
    Transforme les données météo en :
    - table de faits météo avec score calculé
    - dimension ville

    Returns:
        str: Chemin vers la table de faits météo
    """
    # 1. Chemins
    input_file = "./data/processed/meteo_global.csv"
    output_dir = "./data/star_schema"
    os.makedirs(output_dir, exist_ok=True)

    # 2. Chargement
    meteo_df = pd.read_csv(input_file)

    # 3. Nettoyage de colonnes (on garde l’essentiel)
    meteo_df = meteo_df[[
        "datetime", "temp", "humidity", "wind_speed", "rain_prob",
        "description", "city_name", "city_country", "date_extraction"
    ]]

    # 4. Création dimension ville (clé primaire : ville_id)
    dim_ville_path = f"{output_dir}/dim_ville.csv"

    if os.path.exists(dim_ville_path):
        dim_ville = pd.read_csv(dim_ville_path)
    else:
        dim_ville = pd.DataFrame(columns=["ville_id", "city_name", "city_country"])

    villes_existantes = set(zip(dim_ville["city_name"], dim_ville["city_country"]))
    nouvelles_villes = set(zip(meteo_df["city_name"], meteo_df["city_country"])) - villes_existantes

    if nouvelles_villes:
        next_id = dim_ville["ville_id"].max() + 1 if not dim_ville.empty else 1
        new_rows = pd.DataFrame({
            "ville_id": range(next_id, next_id + len(nouvelles_villes)),
            "city_name": [v[0] for v in nouvelles_villes],
            "city_country": [v[1] for v in nouvelles_villes]
        })
        dim_ville = pd.concat([dim_ville, new_rows], ignore_index=True)
        dim_ville.to_csv(dim_ville_path, index=False)

    # 5. Jointure des villes (clé étrangère dans la table de faits)
    facts_df = meteo_df.merge(dim_ville, on=["city_name", "city_country"], how="left")


    # 7. Construction de la table de faits
    facts_df = facts_df[[
        "datetime", "temp", "humidity", "wind_speed", "rain_prob",
        "description", "ville_id", "date_extraction"
    ]]

    # 8. Sauvegarde
    fact_path = f"{output_dir}/fact_weather.csv"
    facts_df.to_csv(fact_path, index=False)

    print("✅ Schéma en étoile généré avec score météo.")
    return fact_path

#transform_to_star_schema_with_score()