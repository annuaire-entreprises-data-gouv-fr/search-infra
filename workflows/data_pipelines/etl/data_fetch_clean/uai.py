import pandas as pd
from config import URL_UAI


def preprocess_uai_data(data_dir):
    df_uai = pd.read_csv(URL_UAI, dtype=str)
    df_list_uai = (
        df_uai.groupby(["siret"])["uai"].apply(list).reset_index(name="liste_uai")
    )
    df_list_uai = df_list_uai[["siret", "liste_uai"]]
    df_list_uai["liste_uai"] = df_list_uai["liste_uai"].astype(str)
    del df_uai

    return df_list_uai
