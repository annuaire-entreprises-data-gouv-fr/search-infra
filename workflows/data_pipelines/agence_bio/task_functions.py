import pandas as pd
import logging
from typing import Any

from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.helpers.utils import flatten_object
from dag_datalake_sirene.config import (
    AGENCE_BIO_TMP_FOLDER,
)
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.workflows.data_pipelines.agence_bio.bio_client import (
    BIOAPIClient,
)


def process_agence_bio_data(data: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    df_bio = pd.DataFrame(data)
    df_bio["flatten_activites"] = df_bio["activites"].apply(
        lambda x: flatten_object(x, "nom")
    )

    arr_cert = []
    arr_prod = []
    arr_adr = []
    for _, row in df_bio.iterrows():
        for cert in row["certificats"]:
            dict_cert = cert.copy()
            dict_cert["siret"] = row["siret"]
            dict_cert["id_bio"] = row["numeroBio"]
            arr_cert.append(dict_cert)
        for prod in row["productions"]:
            dict_prod = prod.copy()
            dict_prod["siret"] = row["siret"]
            dict_prod["id_bio"] = row["numeroBio"]
            arr_prod.append(dict_prod)
        for adr in row["adressesOperateurs"]:
            dict_adr = adr.copy()
            dict_adr["siret"] = row["siret"]
            dict_adr["id_bio"] = row["numeroBio"]
            arr_adr.append(dict_adr)

    df_cert = pd.DataFrame(arr_cert)
    df_prod = pd.DataFrame(arr_prod)
    df_adr = pd.DataFrame(arr_adr)

    # Process df_cert
    df_cert = df_cert[
        [
            "id_bio",
            "siret",
            "organisme",
            "etatCertification",
            "dateSuspension",
            "dateArret",
            "dateEngagement",
            "dateNotification",
            "url",
        ]
    ]
    df_cert = df_cert.rename(
        columns={
            "etatCertification": "etat_certification",
            "dateSuspension": "date_suspension",
            "dateArret": "date_arret",
            "dateEngagement": "date_engagement",
            "dateNotification": "date_notification",
        }
    )

    # Process df_prod
    df_prod["flatten_etatProductions"] = df_prod["etatProductions"].apply(
        lambda x: flatten_object(x, "etatProduction")
    )
    df_prod = df_prod[["id_bio", "siret", "code", "nom", "flatten_etatProductions"]]
    df_prod = df_prod.rename(columns={"flatten_etatProductions": "etat_productions"})

    # Process df_adr
    df_adr = df_adr[
        [
            "id_bio",
            "siret",
            "lieu",
            "codePostal",
            "ville",
            "lat",
            "long",
            "codeCommune",
            "active",
            "departementId",
            "typeAdresseOperateurs",
        ]
    ]
    df_adr = df_adr.rename(
        columns={
            "lieu": "adresse",
            "codePostal": "code_postal",
            "ville": "commune",
            "codeCommune": "code_commune",
            "departementId": "code_departement",
            "typeAdresseOperateurs": "type_adresse_operateurs",
        }
    )

    # Process df_bio
    df_bio = df_bio.drop(
        columns=["adressesOperateurs", "productions", "activites", "certificats"]
    )
    df_bio = df_bio.rename(
        columns={
            "numeroBio": "id_bio",
            "raisonSociale": "nom_raison_sociale",
            "denominationcourante": "denomination_courante",
            "codeNAF": "code_naf",
            "dateMaj": "date_maj",
            "telephoneCommerciale": "telephone_commerciale",
            "siteWebs": "site_webs",
            "flatten_activites": "activites",
        }
    )
    df_bio = df_bio[
        [
            "id_bio",
            "siret",
            "nom_raison_sociale",
            "denomination_courante",
            "telephone",
            "email",
            "code_naf",
            "gerant",
            "date_maj",
            "telephone_commerciale",
            "reseau",
            "categories",
            "site_webs",
            "mixite",
            "activites",
        ]
    ]

    return {
        "principal": df_bio,
        "certifications": df_cert,
        "productions": df_prod,
        "adresses": df_adr,
    }


def process_agence_bio(ti):
    client = BIOAPIClient()

    # Fetch data
    logging.info("Fetching data from BIO API...")
    raw_data = client.fetch_all_departements()
    logging.info(f"Fetched {len(raw_data)} records from BIO API")

    # Process data
    logging.info("Processing BIO API data...")
    processed_data = process_agence_bio_data(raw_data)

    # Save to CSV
    for name, df in processed_data.items():
        file_path = f"{AGENCE_BIO_TMP_FOLDER}agence_bio_{name}.csv"
        df.to_csv(file_path, index=False)
        logging.info(f"Saved {name} data to {file_path}")

    ti.xcom_push(
        key="nb_id_bio", value=str(processed_data["principal"]["id_bio"].nunique())
    )
    ti.xcom_push(
        key="nb_siret", value=str(processed_data["principal"]["siret"].nunique())
    )


def send_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": AGENCE_BIO_TMP_FOLDER,
                "source_name": "agence_bio_principal.csv",
                "dest_path": "agence_bio/new/",
                "dest_name": "agence_bio_principal.csv",
            },
            {
                "source_path": AGENCE_BIO_TMP_FOLDER,
                "source_name": "agence_bio_certifications.csv",
                "dest_path": "agence_bio/new/",
                "dest_name": "agence_bio_certifications.csv",
            },
            {
                "source_path": AGENCE_BIO_TMP_FOLDER,
                "source_name": "agence_bio_productions.csv",
                "dest_path": "agence_bio/new/",
                "dest_name": "agence_bio_productions.csv",
            },
            {
                "source_path": AGENCE_BIO_TMP_FOLDER,
                "source_name": "agence_bio_adresses.csv",
                "dest_path": "agence_bio/new/",
                "dest_name": "agence_bio_adresses.csv",
            },
        ],
    )


def compare_files_minio():
    is_same = minio_client.compare_files(
        file_path_1="agence_bio/new/",
        file_name_2="agence_bio_principal.csv",
        file_path_2="agence_bio/latest/",
        file_name_1="agence_bio_principal.csv",
    )
    if is_same:
        return False

    if is_same is None:
        logging.info("First time in this Minio env. Creating")

    minio_client.send_files(
        list_files=[
            {
                "source_path": AGENCE_BIO_TMP_FOLDER,
                "source_name": "agence_bio_principal.csv",
                "dest_path": "agence_bio/latest/",
                "dest_name": "agence_bio_principal.csv",
            },
            {
                "source_path": AGENCE_BIO_TMP_FOLDER,
                "source_name": "agence_bio_certifications.csv",
                "dest_path": "agence_bio/latest/",
                "dest_name": "agence_bio_certifications.csv",
            },
            {
                "source_path": AGENCE_BIO_TMP_FOLDER,
                "source_name": "agence_bio_productions.csv",
                "dest_path": "agence_bio/latest/",
                "dest_name": "agence_bio_productions.csv",
            },
            {
                "source_path": AGENCE_BIO_TMP_FOLDER,
                "source_name": "agence_bio_adresses.csv",
                "dest_path": "agence_bio/latest/",
                "dest_name": "agence_bio_adresses.csv",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_id_bio = ti.xcom_pull(key="nb_id_bio", task_ids="process_agence_bio")
    nb_siret = ti.xcom_pull(key="nb_siret", task_ids="process_agence_bio")
    send_message(
        f"\U0001F7E2 Données Agence Bio (certificats professionnels Bio) "
        f"mises à jour.\n"
        f"- {nb_id_bio} identifiants Bio référencés.\n"
        f"- {nb_siret} établissements (siret) représentés."
    )
