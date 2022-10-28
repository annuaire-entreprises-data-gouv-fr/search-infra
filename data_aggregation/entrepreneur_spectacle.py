import os

import pandas as pd
import requests


def preprocess_spectacle_data(
    data_dir,
) -> None:
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)

    r = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/fb6c3b2e-da8c-4e69-a719-6a96329e4cb2"
    )
    with open(data_dir + "spectacle-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_spectacle = pd.read_csv(data_dir + "spectacle-download.csv", dtype=str, sep=";")
    df_spectacle = df_spectacle[df_spectacle["statut_du_recepisse"] == "Valide"]
    df_spectacle["is_entrepreneur_spectacle"] = True
    df_spectacle["siren"] = df_spectacle[
        "siren_personne_physique_siret_personne_morale"
    ].str[:9]
    df_spectacle = df_spectacle[["siren", "is_entrepreneur_spectacle"]]
    df_spectacle = df_spectacle[df_spectacle["siren"].notna()]
    df_spectacle.to_csv(data_dir + "spectacle-new.csv", index=False)


def generate_updates_spectacle(df_spectacle, current_color):
    for index, row in df_spectacle.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "doc": {
                "is_entrepreneur_spectacle": True,
            },
        }
