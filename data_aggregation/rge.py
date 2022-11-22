import logging
import os

import pandas as pd
import requests


def preprocess_rge_data(
    data_dir,
) -> None:
    from typing import List

    os.makedirs(os.path.dirname(data_dir), exist_ok=True)
    list_rge: List[str] = []
    r = requests.get(
        "https://data.ademe.fr/data-fair/api/v1/datasets/liste-des-entreprises-rge-2/"
        "lines?size=10000&select=siret%2Ccode_qualification"
    )
    data = r.json()
    list_rge = list_rge + data["results"]
    cpt = 0
    logging.info(data)
    while "next" in data:
        cpt = cpt + 1
        r = requests.get(data["next"])
        data = r.json()
        list_rge = list_rge + data["results"]
    df_rge = pd.DataFrame(list_rge)
    df_rge["siren"] = df_rge["siret"].str[:9]
    df_rge = df_rge[["siren", "siret", "code_qualification"]]
    df_rge.to_csv(data_dir + "rge-new.csv", index=False)


def generate_updates_rge(df_rge, current_color):
    for index, row in df_rge.iterrows():
        yield {
            "_op_type": "update",
            "_index": "siren-" + current_color,
            "_type": "_doc",
            "_id": row["siren"],
            "script": {
                "source": "def targets = ctx._source.etablissements.findAll("
                          "etablissement -> etablissement.siret == params.siret); "
                          "for(etablissement in targets)"
                          "{etablissement.id_rge = params.id_rge}",
                "params": {
                    "siret": row["siret"],
                    "id_rge": row["code_qualification"],

                },
            },
        }
