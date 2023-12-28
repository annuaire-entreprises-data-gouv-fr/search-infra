import pandas as pd
import requests
from dag_datalake_sirene.config import URL_CONVENTION_COLLECTIVE


def preprocess_convcollective_data(data_dir):
    r = requests.get(URL_CONVENTION_COLLECTIVE, allow_redirects=True)
    with open(data_dir + "convcollective-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)
    df_conv_coll = pd.read_csv(
        data_dir + "convcollective-download.csv",
        dtype=str,
        names=["mois", "siret", "idcc", "date_maj"],
        header=0,
    )
    # df_conv_coll["siren"] = df_conv_coll["siret"].str[0:9]
    df_conv_coll = df_conv_coll[df_conv_coll["siret"].notna()]
    df_conv_coll["idcc"] = df_conv_coll["idcc"].apply(lambda x: str(x).replace(" ", ""))
    df_liste_cc = (
        df_conv_coll.groupby(by=["siret"])["idcc"]
        .apply(list)
        .reset_index(name="liste_idcc")
    )
    # df_liste_cc["siren"] = df_liste_cc["siret"].str[0:9]
    df_liste_cc["liste_idcc"] = df_liste_cc["liste_idcc"].astype(str)
    del df_conv_coll

    return df_liste_cc
