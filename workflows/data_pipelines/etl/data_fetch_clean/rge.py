from typing import List
import pandas as pd
import requests
from dag_datalake_sirene.config import URL_RGE


def preprocess_rge_data(**kwargs):
    rge_url = URL_RGE
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
