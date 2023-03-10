import numpy as np
import pandas as pd


def preprocess_egapro_data(data_dir):
    df_egapro = pd.read_excel(
        "https://www.data.gouv.fr/fr/datasets/r/d434859f-8d3b-4381-bcdb-ec9200653ae6",
        dtype=str,
        engine="openpyxl"
    )
    df_egapro = df_egapro.sort_values(by=["Année"])
    df_egapro = df_egapro.drop_duplicates(subset=["SIREN"], keep="last")
    df_egapro = df_egapro[["SIREN", "Note Index"]]
    df_egapro = df_egapro[df_egapro["Note Index"] != "NC"]
    df_egapro["Note Index"] = df_egapro["Note Index"].fillna(np.nan).astype(float)
    df_egapro = df_egapro.rename(
        columns={"SIREN": "siren", "Note Index": "note_egapro"}
    )
    return df_egapro
