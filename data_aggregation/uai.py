import os

import pandas as pd
import requests


def preprocess_uai_data(
    data_dir,
) -> None:
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)

    r = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/b22f04bf-64a8-495d-b8bb-d84dbc4c7983"
    )
    with open(data_dir + "uai-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_uai = pd.read_csv(data_dir + "uai-download.csv", dtype=str, sep=";")
    df_uai = df_uai[["identifiant_de_l_etablissement", "siren_siret", "code_nature"]]
    df_uai = df_uai.rename(
        columns={"identifiant_de_l_etablissement": "uai", "siren_siret": "siren"}
    )
    df_uai["siren"] = df_uai["siren"].str[:9]
    df_uai = df_uai[["siren", "siret", "uai"]]
    df_uai.to_csv(data_dir + "uai-new.csv", index=False)


def generate_updates_uai(df_uai, current_color):
    for index, row in df_uai.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "script": {
                "source": "def targets = ctx._source.etablissements.findAll("
                "etablissement -> etablissement.siret == params.siret); "
                "for(etablissement in targets)"
                "{etablissement.id_uai = params.id_uai}",
                "params": {
                    "siret": row["siret"],
                    "id_uai": row["uai"],

                },
            },
        }
