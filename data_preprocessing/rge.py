from typing import List

import pandas as pd
import requests


def preprocess_rge_data():
    rge_url = (
        "https://data.ademe.fr/data-fair/api/v1/datasets/liste-des-entreprises-rge-2/"
        "lines?size=10000&select=siret%2Ccode_qualification"
    )
    r = requests.get(rge_url, allow_redirects=True)
    data = r.json()
    list_rge: List[str] = []
    list_rge = list_rge + data["results"]
    cpt = 0
    while "next" in data:
        cpt = cpt + 1
        r = requests.get(data["next"])
        data = r.json()
        list_rge = list_rge + data["results"]
    df_rge = pd.DataFrame(list_rge)
    # df_rge["siren"] = df_rge["siret"].str[:9]
    df_rge = df_rge[df_rge["siret"].notna()]
    df_list_rge = (
        df_rge.groupby(["siret"])["code_qualification"]
        .apply(list)
        .reset_index(name="liste_rge")
    )
    df_list_rge = df_list_rge[["siret", "liste_rge"]]
    df_list_rge["liste_rge"] = df_list_rge["liste_rge"].astype(str)
    del df_rge
    return df_list_rge
