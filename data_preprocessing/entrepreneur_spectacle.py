import pandas as pd
import requests


def preprocess_spectacle_data(data_dir):
    r = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/fb6c3b2e-da8c-4e69-a719-6a96329e4cb2"
    )
    with open(data_dir + "spectacle-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_spectacle = pd.read_csv(data_dir + "spectacle-download.csv", dtype=str, sep=";")
    df_spectacle["est_entrepreneur_spectacle"] = True
    df_spectacle["siren"] = df_spectacle[
        "siren_personne_physique_siret_personne_morale"
    ].str[:9]
    df_spectacle = df_spectacle[
        [
            "siren",
            "est_entrepreneur_spectacle",
            "statut_du_recepisse",
            "date_de_depot_de_la_declaration_inscrite_sur_le_recepisse",
        ]
    ]
    df_spectacle = df_spectacle.rename(
        columns={
            "statut_du_recepisse": "statut_entrepreneur_spectacle",
            "date_de_depot_de_la_declaration_inscrite_sur_le_recepisse": "date",
        }
    )
    df_spectacle["statut_entrepreneur_spectacle"] = df_spectacle[
        "statut_entrepreneur_spectacle"
    ].apply(lambda x: "valide" if x == "Valide" else "invalide")
    df_spectacle = df_spectacle[df_spectacle["siren"].notna()]
    df_spectacle["date"] = pd.to_datetime(df_spectacle["date"])
    # Only keep the latest entry
    df_spectacle_processed = (
        df_spectacle.sort_values(by="date", ascending=False)
        .groupby("siren")
        .first()
        .reset_index()
    )
    return df_spectacle_processed
