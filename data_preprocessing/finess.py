import pandas as pd
import requests


def preprocess_finess_data(data_dir):
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
    # df_finess["siren"] = df_finess["siren"].str[:9]
    df_finess = df_finess[df_finess["siret"].notna()]
    df_list_finess = (
        df_finess.groupby(["siret"])["finess"]
        .apply(list)
        .reset_index(name="liste_finess")
    )
    df_list_finess = df_list_finess[["siret", "liste_finess"]]
    df_list_finess["liste_finess"] = df_list_finess["liste_finess"].astype(str)
    del df_finess

    return df_list_finess
