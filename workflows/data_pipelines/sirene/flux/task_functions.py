from datetime import datetime
import pandas as pd
import logging
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.helpers.utils import flatten_dict, save_dataframe
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.insee_client import (
    INSEEAPIClient,
)
from dag_datalake_sirene.helpers.tchap import send_message
from dag_datalake_sirene.config import (
    INSEE_API_URL,
    INSEE_FLUX_TMP_FOLDER,
    INSEE_SECRET_BEARER,
)


CURRENT_MONTH = datetime.today().strftime("%Y-%m")


INSEEAPIClient = INSEEAPIClient(INSEE_API_URL, INSEE_SECRET_BEARER)


def get_current_flux_unite_legale(ti):
    endpoint = (
        f"siren?q=dateDernierTraitementUniteLegale%3A{CURRENT_MONTH}"
        "&champs=siren,dateCreationUniteLegale,sigleUniteLegale,prenomUsuelUniteLegale,"
        "identifiantAssociationUniteLegale,trancheEffectifsUniteLegale,"
        "dateDernierTraitementUniteLegale,categorieEntreprise,"
        "etatAdministratifUniteLegale,nomUniteLegale,nomUsageUniteLegale,"
        "denominationUniteLegale,denominationUsuelle1UniteLegale,"
        "denominationUsuelle2UniteLegale,denominationUsuelle3UniteLegale,"
        "categorieJuridiqueUniteLegale,activitePrincipaleUniteLegale,"
        "economieSocialeSolidaireUniteLegale,statutDiffusionUniteLegale,"
        "societeMissionUniteLegale,anneeCategorieEntreprise,anneeEffectifsUniteLegale,caractereEmployeurUniteLegale,"
        "nicSiegeUniteLegale&nombre=1000"
    )

    data = INSEEAPIClient.call_insee_api(
        endpoint=endpoint, data_property="unitesLegales"
    )
    flux = [
        flatten_dict({**entry, **entry.get("periodesUniteLegale", [{}])[0]})
        for entry in data
    ]
    df = pd.DataFrame(flux)

    file_path = f"{INSEE_FLUX_TMP_FOLDER}flux_unite_legale_{CURRENT_MONTH}.csv"
    save_dataframe(df, file_path)

    logging.info(f"******** Nombre flux unités légales : {str(df.shape[0])}")
    ti.xcom_push(key="nb_flux_unite_legale", value=str(df.shape[0]))


def get_stock_non_diffusible(ti):
    endpoint = "siret?q=statutDiffusionUniteLegale%3AP" "&champs=siren&nombre=1000"
    data = INSEEAPIClient.call_insee_api(
        endpoint=endpoint, data_property="etablissements"
    )
    df = pd.DataFrame(data)

    file_path = f"{INSEE_FLUX_TMP_FOLDER}stock_non_diffusible.csv"
    save_dataframe(df, file_path)
    logging.info(f"******** Nombre stock ul non diffusibles : {str(df.shape[0])}")
    ti.xcom_push(key="nb_stock_non_diffusible", value=str(df.shape[0]))


def get_current_flux_non_diffusible(ti):
    endpoint = (
        "siren?q=statutDiffusionUniteLegale%3AP"
        "%20AND%20dateDernierTraitementUniteLegale%3A"
        f"{CURRENT_MONTH}&champs=siren&nombre=1000"
    )
    data = INSEEAPIClient.call_insee_api(endpoint, "unitesLegales")
    df = pd.DataFrame(data)

    file_path = f"{INSEE_FLUX_TMP_FOLDER}flux_non_diffusible_{CURRENT_MONTH}.csv"
    save_dataframe(df, file_path)

    logging.info(f"******** Nombre flux ul non diffusibles : {str(df.shape[0])}")
    ti.xcom_push(key="nb_flux_non_diffusible", value=str(df.shape[0]))


def get_current_flux_etablissement(ti):
    endpoint = (
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
        "&nombre=1000"
    )

    data = INSEEAPIClient.call_insee_api(endpoint, "etablissements")

    flux = [
        flatten_dict({**d, **d.get("periodesEtablissement", [{}])[0]}) for d in data
    ]
    df = pd.DataFrame(flux)

    for prefix in ["adresseEtablissement_", "adresse2Etablissement_"]:
        df.columns = [
            col.replace(prefix, "") if prefix in col else col for col in df.columns
        ]

    file_path = f"{INSEE_FLUX_TMP_FOLDER}flux_etablissement_{CURRENT_MONTH}.csv"
    save_dataframe(df, file_path)

    logging.info(f"******** Nombre flux établissements : {str(df.shape[0])}")
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
