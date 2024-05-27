import pandas as pd
import logging
import requests

from dag_datalake_sirene.helpers.s3_helpers import s3_client
from dag_datalake_sirene.config import (
    CC_TMP_FOLDER,
    URL_CONVENTION_COLLECTIVE,
)
from dag_datalake_sirene.helpers.tchap import send_message


def preprocess_convcollective_data(ti):
    r = requests.get(URL_CONVENTION_COLLECTIVE, allow_redirects=True)
    with open(f"{CC_TMP_FOLDER}convcollective-download.csv", "wb") as f:
        for chunk in r.iter_content(1024):
            f.write(chunk)

    df_conv_coll = pd.read_csv(
        f"{CC_TMP_FOLDER}convcollective-download.csv",
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

    df_cc = merged_df.merge(df_liste_cc, on="siren", how="left")
    df_cc["liste_idcc_by_siren"] = df_cc["liste_idcc_by_siren"].astype(str)
    df_cc["liste_idcc"] = df_cc["liste_idcc"].astype(str)
    df_cc.to_csv(f"{CC_TMP_FOLDER}cc.csv", index=False)
    ti.xcom_push(key="nb_siren_cc", value=str(df_cc["siren"].nunique()))

    del df_liste_cc_by_siren
    del df_liste_cc_by_siret
    del merged_df
    del df_cc


def send_file_to_minio():
    s3_client.send_files(
        list_files=[
            {
                "source_path": CC_TMP_FOLDER,
                "source_name": "cc.csv",
                "dest_path": "convention_collective/new/",
                "dest_name": "cc.csv",
            },
        ],
    )


def compare_files_minio():
    is_same = s3_client.compare_files(
        file_path_1="convention_collective/new/",
        file_name_2="cc.csv",
        file_path_2="convention_collective/latest/",
        file_name_1="cc.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    s3_client.send_files(
        list_files=[
            {
                "source_path": CC_TMP_FOLDER,
                "source_name": "cc.csv",
                "dest_path": "convention_collective/latest/",
                "dest_name": "cc.csv",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siren = ti.xcom_pull(key="nb_siren_cc", task_ids="preprocess_cc_data")
    send_message(
        f"\U0001F7E2 Données Conventions collectives mises à jour.\n"
        f"- {nb_siren} unités légales représentées."
    )
