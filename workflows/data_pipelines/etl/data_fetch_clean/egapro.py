import pandas as pd
from dag_datalake_sirene.config import URL_EGAPRO


def preprocess_egapro_data(data_dir):
    df_egapro = pd.read_excel(
        URL_EGAPRO,
        dtype=str,
        engine="openpyxl",
    )
    df_egapro = df_egapro.drop_duplicates(subset=["SIREN"], keep="first")
    df_egapro = df_egapro[["SIREN"]]
    df_egapro["egapro_renseignee"] = True
    df_egapro = df_egapro.rename(columns={"SIREN": "siren"})
    return df_egapro
