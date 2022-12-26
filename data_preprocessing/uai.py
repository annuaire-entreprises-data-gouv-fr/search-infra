from ast import literal_eval

import pandas as pd
import requests


def preprocess_uai_data(data_dir):

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
    df_list_uai = (
        df_uai.groupby(["siren"])["uai"].apply(list).reset_index(name="liste_uai")
    )
    df_list_uai = df_list_uai[["siren", "liste_uai"]]
    df_list_uai["liste_uai"] = df_list_uai["liste_uai"].astype(str)
    del df_uai

    return df_list_uai
