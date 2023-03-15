import pandas as pd


def preprocess_egapro_data(data_dir):
    df_egapro = pd.read_excel(
        "https://www.data.gouv.fr/fr/datasets/r/d434859f-8d3b-4381-bcdb-ec9200653ae6",
        dtype=str,
        engine="openpyxl",
    )
    df_egapro = df_egapro.drop_duplicates(subset=["SIREN"], keep="first")
    df_egapro = df_egapro[["SIREN"]]
    df_egapro["egapro_renseignee"] = True
    df_egapro = df_egapro.rename(columns={"SIREN": "siren"})
    return df_egapro
