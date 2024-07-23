from datetime import datetime
import gzip
import pandas as pd
import requests
import logging
from requests.adapters import HTTPAdapter, Retry
import time
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.config import (
    INSEE_API_URL,
    INSEE_FLUX_TMP_FOLDER,
    INSEE_SECRET_BEARER,
)


CURRENT_MONTH = datetime.today().strftime("%Y-%m")

session = requests.Session()
retry = Retry(connect=3, backoff_factor=3)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


def flatten_dict(dd, separator="_", prefix=""):
    return (
        {
            prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
        }
        if isinstance(dd, dict)
        else {prefix: dd}
    )


def call_insee_api(api_endpoint, data_property, max_retries=10):
    current_cursor = "*"
    api_data = []
    api_headers = {"Authorization": f"Bearer {INSEE_SECRET_BEARER}"}
    continue_fetching = True
    request_count = 0

    while continue_fetching:
        request_count += 1000
        if request_count % 10000 == 0:
            logging.info(request_count)

        retry_attempt = 0
        while retry_attempt <= max_retries:
            try:
                api_response = session.get(
                    api_endpoint + current_cursor, headers=api_headers
                )
                if api_response.status_code == 429:
                    logging.warning("Rate limit exceeded. Sleeping for 10 seconds...")
                    time.sleep(10)
                    retry_attempt += 1
                    continue
                else:
                    break
            except requests.RequestException as e:
                logging.error(f"An error occurred: {e}")
                time.sleep(30)  # Sleep before retrying in case of request exceptions
                retry_attempt += 1
        else:
            # If while loop exited without breaking, it means retries were exhausted
            raise Exception(f"Max retries exceeded for endpoint {api_endpoint}.")

        response_json = api_response.json()

        if (
            "header" in response_json
            and "curseurSuivant" in response_json["header"]
            and "curseur" in response_json["header"]
            and response_json["header"]["curseur"]
            != response_json["header"]["curseurSuivant"]
        ):
            current_cursor = response_json["header"]["curseurSuivant"]
        else:
            continue_fetching = False

        if data_property in response_json:
            api_data.extend(response_json[data_property])

    return api_data


def get_current_flux_unite_legale(ti):
    query_params = (
        f"siren?q=dateDernierTraitementUniteLegale%3A{CURRENT_MONTH}"
        "&champs=siren,dateCreationUniteLegale,sigleUniteLegale,prenomUsuelUniteLegale,"
        "identifiantAssociationUniteLegale,trancheEffectifsUniteLegale,"
        "dateDernierTraitementUniteLegale,categorieEntreprise,"
        "etatAdministratifUniteLegale,nomUniteLegale,nomUsageUniteLegale,"
        "denominationUniteLegale,denominationUsuelle1UniteLegale,"
        "denominationUsuelle2UniteLegale,denominationUsuelle3UniteLegale,"
        "categorieJuridiqueUniteLegale,activitePrincipaleUniteLegale,"
        "economieSocialeSolidaireUniteLegale,statutDiffusionUniteLegale,"
        "societeMissionUniteLegale,anneeCategorieEntreprise,anneeEffectifsUniteLegale,"
        "caractereEmployeurUniteLegale&nombre=1000&curseur="
    )
    endpoint = f"{INSEE_API_URL}{query_params}"

    data = call_insee_api(endpoint, "unitesLegales")

    flux = []
    for entry in data:
        row = entry.copy()
        if "periodesUniteLegale" in row:
            row.update(row["periodesUniteLegale"][0])
            del row["periodesUniteLegale"]
        flux.append(flatten_dict(row))

    df = pd.DataFrame(flux)

    file_path = f"{INSEE_FLUX_TMP_FOLDER}flux_unite_legale_{CURRENT_MONTH}.csv.gz"
    df.to_csv(file_path, index=False, compression="gzip")

    ti.xcom_push(key="nb_flux_unite_legale", value=str(df.shape[0]))


def get_stock_non_diffusible(ti):
    endpoint = (
        f"{INSEE_API_URL}siret"
        "?q=statutDiffusionUniteLegale%3AP"
        "&champs=siren&nombre=1000&curseur="
    )
    data = call_insee_api(endpoint, "etablissements")
    df = pd.DataFrame(data)
    df.to_csv(
        f"{INSEE_FLUX_TMP_FOLDER}stock_non_diffusible.csv.gz",
        index=False,
        compression="gzip",
    )
    ti.xcom_push(key="nb_stock_non_diffusible", value=str(df.shape[0]))


def get_current_flux_non_diffusible(ti):
    endpoint = (
        f"{INSEE_API_URL}siren"
        "?q=statutDiffusionUniteLegale%3AP"
        "%20AND%20dateDernierTraitementUniteLegale%3A"
        f"{CURRENT_MONTH}&champs=siren&nombre=1000&curseur="
    )
    data = call_insee_api(endpoint, "unitesLegales")
    df = pd.DataFrame(data)
    df.to_csv(
        f"{INSEE_FLUX_TMP_FOLDER}flux_non_diffusible_{CURRENT_MONTH}.csv.gz",
        index=False,
        compression="gzip",
    )
    ti.xcom_push(key="nb_flux_non_diffusible", value=str(df.shape[0]))


def get_current_flux_etablissement(ti):
    query_params = (
        f"siret?q=dateDernierTraitementEtablissement%3A{CURRENT_MONTH}"
        "&champs=siren,siret,dateCreationEtablissement,trancheEffectifsEtablissement,"
        "activitePrincipaleRegistreMetiersEtablissement,etablissementSiege,"
        "numeroVoieEtablissement,dateDernierTraitementEtablissement,"
        "anneeEffectifsEtablissement,libelleVoieEtablissement,codePostalEtablissement,"
        "libelleCommuneEtablissement,libelleCedexEtablissement,typeVoieEtablissement,"
        "codeCommuneEtablissement,codeCedexEtablissement,"
        "complementAdresseEtablissement,distributionSpecialeEtablissement,"
        "complementAdresse2Etablissement,indiceRepetition2Etablissement,"
        "libelleCedex2Etablissement,codeCedex2Etablissement,"
        "numeroVoie2Etablissement,typeVoie2Etablissement,libelleVoie2Etablissement,"
        "codeCommune2Etablissement,libelleCommune2Etablissement,"
        "distributionSpeciale2Etablissement,dateDebut,etatAdministratifEtablissement,"
        "enseigne1Etablissement,enseigne1Etablissement,enseigne2Etablissement,"
        "enseigne3Etablissement,denominationUsuelleEtablissement,"
        "activitePrincipaleEtablissement,indiceRepetitionEtablissement,"
        "libelleCommuneEtrangerEtablissement,codePaysEtrangerEtablissement,"
        "libellePaysEtrangerEtablissement,libelleCommuneEtranger2Etablissement,"
        "codePaysEtranger2Etablissement,libellePaysEtranger2Etablissement,"
        "statutDiffusionEtablissement,"
        "caractereEmployeurEtablissement,"
        "coordonneeLambertAbscisseEtablissement,"
        "coordonneeLambertOrdonneeEtablissement"
        "&nombre=1000&curseur="
    )
    endpoint = f"{INSEE_API_URL}{query_params}"
    data = call_insee_api(endpoint, "etablissements")
    flux = []
    for d in data:
        row = d.copy()
        if "periodesEtablissement" in row:
            for item in row["periodesEtablissement"][0]:
                row[item] = row["periodesEtablissement"][0][item]
            del row["periodesEtablissement"]
        flux.append(flatten_dict(row))

    data.clear()

    # We save csv.gz by batch of 100 000 for memory
    df = pd.DataFrame(columns=[c for c in flux[0]])
    for column in df.columns:
        for prefix in ["adresseEtablissement_", "adresse2Etablissement_"]:
            if prefix in column:
                df = df.rename(columns={column: column.replace(prefix, "")})
    file_path = f"{INSEE_FLUX_TMP_FOLDER}flux_etablissement_{CURRENT_MONTH}.csv"
    df.to_csv(
        file_path,
        index=False,
    )
    first = 0
    for i in range(len(flux)):
        if i != 0 and i % 100000 == 0:
            fluxinter = flux[first:i]
            df = pd.DataFrame(fluxinter)
            df.to_csv(
                file_path,
                mode="a",
                index=False,
                header=False,
            )
            first = i

    fluxinter = flux[first : len(flux)]
    df = pd.DataFrame(fluxinter)
    df.to_csv(
        file_path,
        mode="a",
        index=False,
        header=False,
    )

    with open(file_path, "rb") as orig_file:
        with gzip.open(
            f"{file_path}.gz",
            "wb",
        ) as zipped_file:
            zipped_file.writelines(orig_file)

    ti.xcom_push(key="nb_flux_etablissement", value=str(df.shape[0]))


def send_flux_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": INSEE_FLUX_TMP_FOLDER,
                "source_name": f"flux_unite_legale_{CURRENT_MONTH}.csv.gz",
                "dest_path": "insee/sirene/flux/",
                "dest_name": f"flux_unite_legale_{CURRENT_MONTH}.csv.gz",
            },
            {
                "source_path": INSEE_FLUX_TMP_FOLDER,
                "source_name": f"flux_etablissement_{CURRENT_MONTH}.csv.gz",
                "dest_path": "insee/sirene/flux/",
                "dest_name": f"flux_etablissement_{CURRENT_MONTH}.csv.gz",
            },
            {
                "source_path": INSEE_FLUX_TMP_FOLDER,
                "source_name": f"flux_non_diffusible_{CURRENT_MONTH}.csv.gz",
                "dest_path": "insee/sirene/flux/",
                "dest_name": f"flux_non_diffusible_{CURRENT_MONTH}.csv.gz",
            },
        ],
    )


def send_stock_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": INSEE_FLUX_TMP_FOLDER,
                "source_name": "stock_non_diffusible.csv.gz",
                "dest_path": "insee/sirene/flux/",
                "dest_name": "stock_non_diffusible.csv.gz",
            }
        ],
    )


def send_notification(ti):
    nb_flux_non_diffusible = ti.xcom_pull(
        key="nb_flux_non_diffusible", task_ids="get_current_flux_non_diffusible"
    )
    nb_flux_unite_legale = ti.xcom_pull(
        key="nb_flux_unite_legale", task_ids="get_current_flux_unite_legale"
    )
    nb_flux_etablissement = ti.xcom_pull(
        key="nb_flux_etablissement", task_ids="get_current_flux_etablissement"
    )
    send_message(
        f"\U0001F7E2 Données Flux Sirene mises à jour - Disponibles sur Minio - Bucket "
        f"{minio_client.bucket}\n"
        f"- {nb_flux_non_diffusible} unités légales non diffusibles modifiées"
        f" ce mois-ci\n"
        f"- {nb_flux_unite_legale}  unités légales modifiées ce mois-ci\n"
        f"- {nb_flux_etablissement} établissements modifiés ce mois-ci"
    )


def send_notification_stock(ti):
    nb_stock_non_diffusible = ti.xcom_pull(
        key="nb_stock_non_diffusible", task_ids="get_stock_non_diffusible"
    )
    send_message(
        f"\U0001F7E2 Données flux Sirene mises à jour."
        f"- {nb_stock_non_diffusible} unités légales non diffusibles."
    )


def send_notification_failure_tchap(context):
    send_message("\U0001F534 Données :" "\nFail DAG de flux sirene!!!!")
