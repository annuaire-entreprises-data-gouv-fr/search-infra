import os
from ast import literal_eval

import pandas as pd
import requests
import logging


def preprocess_finess_data(
    data_dir,
) -> None:
    os.makedirs(os.path.dirname(data_dir), exist_ok=True)
    r = requests.get(
        "https://www.data.gouv.fr/fr/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"
    )
    with open(data_dir + "finess-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_finess = pd.read_csv(
        data_dir + "finess-download.csv",
        dtype=str,
        sep=";",
        encoding="Latin-1",
        skiprows=1,
        header=None,
    )
    df_finess = df_finess[[1, 18, 22]]
    df_finess = df_finess.rename(
        columns={1: "finess", 18: "cat_etablissement", 22: "siret"}
    )
    df_finess["siren"] = df_finess["siret"].str[:9]
    df_finess = df_finess[df_finess["siren"].notna()]
    df_finess = df_finess[["finess", "siren", "siret"]]
    df_finess.to_csv(data_dir + "finess-new.csv", index=False)


def generate_updates_finess(df_finess, current_color):
    # add id_finess to etablissements nested field
    for index, row in df_finess.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "script": {
                "source": "def targets = ctx._source.etablissements.findAll("
                "etablissement -> etablissement.siret == params.siret); "
                "for(etablissement in targets)"
                "{etablissement.id_finess = params.id_finess}",
                "params": {
                    "siret": row["siret"],
                    "id_finess": row["finess"],

                },
            },
        }
