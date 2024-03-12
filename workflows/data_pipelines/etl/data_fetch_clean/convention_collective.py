import pandas as pd
import requests
from dag_datalake_sirene.config import URL_CONVENTION_COLLECTIVE


def preprocess_convcollective_data(data_dir):
    r = requests.get(URL_CONVENTION_COLLECTIVE, allow_redirects=True)
    with open(data_dir + "convcollective-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    # Read data into DataFrame
    df_conv_coll = pd.read_csv(
        data_dir + "convcollective-download.csv",
        dtype=str,
        names=["mois", "siret", "idcc", "date_maj"],
        header=0,
    )

    # Preprocessing
    df_conv_coll["siren"] = df_conv_coll["siret"].str[0:9]
    df_conv_coll = df_conv_coll.dropna(subset=["siret"])
    df_conv_coll["idcc"] = df_conv_coll["idcc"].str.replace(" ", "")

    df_liste_cc = (
        df_conv_coll.groupby(by=["siren"])["idcc"]
        .unique()
        .apply(list)
        .reset_index(name="liste_idcc")
    )

    df_liste_cc_by_siret = (
        df_conv_coll.groupby(by=["siret"])["idcc"]
        .apply(list)
        .reset_index(name="liste_idcc_by_siret")
    )
    # df_liste_cc["siren"] = df_liste_cc["siret"].str[0:9]
    df_liste_cc_by_siret["liste_idcc_by_siret"] = df_liste_cc_by_siret[
        "liste_idcc_by_siret"
    ].astype(str)
    df_liste_cc_by_siret["siren"] = df_liste_cc_by_siret["siret"].str[0:9]

    # Group by siren and construct the dictionary
    siren_idcc_dict = {}
    for siren, group in df_conv_coll.groupby("siren"):
        idcc_siret_dict = {}
        for _, row in group.iterrows():
            idcc = row["idcc"]
            siret = row["siret"]
            if idcc not in idcc_siret_dict:
                idcc_siret_dict[idcc] = []
            idcc_siret_dict[idcc].append(siret)
        siren_idcc_dict[siren] = idcc_siret_dict

    # Create DataFrame from the dictionary
    df_liste_cc_by_siren = pd.DataFrame(
        siren_idcc_dict.items(), columns=["siren", "liste_idcc_by_siren"]
    )

    merged_df = df_liste_cc_by_siret.merge(df_liste_cc_by_siren, on="siren", how="left")

    merged_df["liste_idcc_by_siren"] = merged_df["liste_idcc_by_siren"].astype(str)

    df_cc = merged_df.merge(df_liste_cc, on="siren", how="left")

    del df_liste_cc_by_siren
    del df_liste_cc_by_siret
    del merged_df

    return df_cc
