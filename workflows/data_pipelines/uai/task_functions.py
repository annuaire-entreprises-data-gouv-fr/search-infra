import pandas as pd
import logging
import os
from datetime import datetime
from dag_datalake_sirene.config import (
    UAI_TMP_FOLDER,
    DATASET_ID_UAI_ONISEP,
    RESOURCE_ID_UAI_MESR,
    RESOURCE_ID_UAI_MENJ,
)
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.helpers.datagouv import (
    get_dataset_or_resource_metadata,
    get_resource,
)
from dag_datalake_sirene.helpers.utils import fetch_last_modified_date, save_to_metadata

from dag_datalake_sirene.helpers.minio_helpers import minio_client


def download_latest_data(ti):

    # https://www.data.gouv.fr/fr/datasets/annuaire-de-leducation/
    get_resource(
        resource_id=RESOURCE_ID_UAI_MENJ,
        file_to_store={"dest_path": UAI_TMP_FOLDER, "dest_name": "menj.csv"},
    )
    # https://www.data.gouv.fr/en/datasets/
    # principaux-etablissements-denseignement-superieur/
    get_resource(
        resource_id=RESOURCE_ID_UAI_MESR,
        file_to_store={"dest_path": UAI_TMP_FOLDER, "dest_name": "mesr.csv"},
    )

    # Les ressources du JDD ONISEP https://www.data.gouv.fr/fr/
    # datasets/5fa5e386afdaa6152360f323/ sont régulièrements écrasés
    # pour de nouvelles ressources. On récupère donc le csv du JDD :
    data = get_dataset_or_resource_metadata(
        dataset_id=DATASET_ID_UAI_ONISEP,
    )
    logging.info(f"+++++++++++{data}")
    for res in data["resources"]:
        if res["format"] == "csv":
            logging.info(f"+++++++++++{res['id']}")
            get_resource(
                resource_id=res["id"],
                file_to_store={"dest_path": UAI_TMP_FOLDER, "dest_name": "onisep.csv"},
            )


def process_uai(ti):
    target_columns = [
        "uai",
        "denomination",
        "sigle",
        "adresse",
        "code_postal",
        "code_commune",
        "commune",
        "siren",
        "siret",
        "public_prive",
        "statut_prive",
        "type",
    ]
    df_menj = pd.read_csv(
        f"{UAI_TMP_FOLDER}menj.csv", dtype=str, sep=";", encoding="Latin-1"
    )
    df_menj = df_menj.rename(
        columns={
            "Identifiant_de_l_etablissement": "uai",
            "Nom_etablissement": "denomination",
            "Adresse_1": "adresse",
            "Code_postal": "code_postal",
            "Code_commune": "code_commune",
            "Nom_commune": "commune",
            "SIREN_SIRET": "siret",
            "Statut_public_prive": "public_prive",
            "Type_contrat_prive": "statut_prive",
            "Type_etablissement": "type",
        }
    )
    df_menj["sigle"] = None
    df_menj["siren"] = df_menj["siret"].str[:9]
    df_menj = df_menj[target_columns]
    df_mesr = pd.read_csv(f"{UAI_TMP_FOLDER}mesr.csv", dtype=str, sep=";")
    df_mesr = df_mesr.rename(
        columns={
            "uai": "uai",
            "uo_lib": "denomination",
            "sigle": "sigle",
            "adresse_uai": "adresse",
            "code_postal_uai": "code_postal",
            "com_code": "code_commune",
            "uucr_nom": "commune",
            "siren": "siren",
            "siret": "siret",
            "com_nom": "public_prive",
            "type_d_etablissement": "type",
        }
    )
    df_mesr["statut_prive"] = None
    df_mesr = df_mesr[target_columns]
    df_onisep = pd.read_csv(f"{UAI_TMP_FOLDER}onisep.csv", dtype=str, sep=";")
    df_onisep = df_onisep.rename(
        columns={
            "code UAI": "uai",
            "nom": "denomination",
            "sigle": "sigle",
            "adresse": "adresse",
            "CP": "code_postal",
            "commune (COG)": "code_commune",
            "commune": "commune",
            "n° SIRET": "siret",
            "statut": "public_prive",
            "type d'établissement": "type",
        }
    )
    df_onisep["siren"] = df_onisep["siret"].str[:9]
    df_onisep["statut_prive"] = None
    df_onisep = df_onisep[target_columns]
    annuaire_uai = pd.concat([df_menj, df_mesr])
    annuaire_uai = pd.concat([annuaire_uai, df_onisep])
    annuaire_uai = annuaire_uai.drop_duplicates(subset=["uai"], keep="first")
    annuaire_uai.to_csv(f"{UAI_TMP_FOLDER}annuaire_uai.csv", index=False)

    ti.xcom_push(key="nb_uai", value=str(annuaire_uai["uai"].nunique()))
    ti.xcom_push(key="nb_siret", value=str(annuaire_uai["siret"].nunique()))


def save_last_modified_date():
    date_list = []
    uai_resource_id_list = [
        RESOURCE_ID_UAI_MESR,
        RESOURCE_ID_UAI_MENJ,
    ]

    for resource_id in uai_resource_id_list:
        date_list.append(fetch_last_modified_date(resource_id))

    # Convert date strings to datetime objects and find the max (Remove timezone offset)
    date_list_dt = [datetime.fromisoformat(date[:-6]) for date in date_list]
    date_last_modified = max(date_list_dt)

    # Format the maximum date back to string if necessary,
    # returns the format 'YYYY-MM-DDTHH:MM:SS.ssssss'
    date_last_modified_str = date_last_modified.isoformat()

    metadata_path = os.path.join(UAI_TMP_FOLDER, "metadata.json")

    save_to_metadata(metadata_path, "last_modified", date_last_modified_str)


def send_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": UAI_TMP_FOLDER,
                "source_name": "annuaire_uai.csv",
                "dest_path": "uai/new/",
                "dest_name": "metadate.json",
            },
            {
                "source_path": UAI_TMP_FOLDER,
                "source_name": "annuaire_uai.csv",
                "dest_path": "uai/new/",
                "dest_name": "metadate.json",
            },
        ],
    )


def compare_files_minio():
    is_same = minio_client.compare_files(
        file_path_1="uai/new/",
        file_name_2="annuaire_uai.csv",
        file_path_2="uai/latest/",
        file_name_1="annuaire_uai.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    minio_client.send_files(
        list_files=[
            {
                "source_path": UAI_TMP_FOLDER,
                "source_name": "annuaire_uai.csv",
                "dest_path": "uai/latest/",
                "dest_name": "annuaire_uai.csv",
            },
            {
                "source_path": UAI_TMP_FOLDER,
                "source_name": "metadata.json",
                "dest_path": "uai/latest/",
                "dest_name": "metadata.json",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_uai = ti.xcom_pull(key="nb_uai", task_ids="process_uai")
    nb_siret = ti.xcom_pull(key="nb_siret", task_ids="process_uai")
    send_message(
        f"\U0001F7E2 Données UAI (établissements scolaires) mises à jour.\n"
        f"- {nb_uai} établissements scolaires référencés.\n"
        f"- {nb_siret} établissements (siret) représentés."
    )
