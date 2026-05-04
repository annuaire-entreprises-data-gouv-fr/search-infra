import logging
import shutil

import pandas as pd
import requests
from airflow.sdk.exceptions import AirflowSkipException
from botocore.exceptions import ClientError

from data_pipelines_annuaire.config import CURRENT_MONTH
from data_pipelines_annuaire.helpers.object_storage import File, ObjectStorageClient
from data_pipelines_annuaire.workflows.data_pipelines.etl.task_functions.determine_sirene_date import (
    get_sirene_processing_month,
)
from data_pipelines_annuaire.workflows.data_pipelines.sirene.flux.config import (
    FLUX_SIRENE_CONFIG,
)
from data_pipelines_annuaire.workflows.data_pipelines.sirene.stock.config import (
    STOCK_SIRENE_CONFIG,
)


def download_historique(data_dir):
    year_month = get_sirene_processing_month()
    filename = STOCK_SIRENE_CONFIG.files_to_download["historique_unite_legale"][
        "destination"
    ].split("/")[-1]
    filename = filename.replace(CURRENT_MONTH, year_month)
    url = STOCK_SIRENE_CONFIG.url_object_storage + filename

    logging.info(f"Downloading and unpacking {url}..")
    r = requests.get(
        url,
        allow_redirects=True,
    )
    open(data_dir + "StockUniteLegaleHistorique_utf8.zip", "wb").write(r.content)
    shutil.unpack_archive(data_dir + "StockUniteLegaleHistorique_utf8.zip", data_dir)
    df_iterator = pd.read_csv(
        f"{data_dir}StockUniteLegaleHistorique_utf8.csv",
        chunksize=100000,
        dtype=str,
    )
    return df_iterator


def download_stock(data_dir):
    year_month = get_sirene_processing_month()
    filename = STOCK_SIRENE_CONFIG.files_to_download["stock_unite_legale"][
        "destination"
    ].split("/")[-1]
    filename = filename.replace(CURRENT_MONTH, year_month)
    url = STOCK_SIRENE_CONFIG.url_object_storage + filename

    logging.info(f"Downloading and unpacking {url}..")
    r = requests.get(
        url,
        allow_redirects=True,
    )
    open(data_dir + "StockUniteLegale_utf8.zip", "wb").write(r.content)
    shutil.unpack_archive(data_dir + "StockUniteLegale_utf8.zip", data_dir)
    df_iterator = pd.read_csv(
        f"{data_dir}StockUniteLegale_utf8.csv", chunksize=100000, dtype=str
    )
    return df_iterator


def download_flux(data_dir):
    year_month = get_sirene_processing_month()
    try:
        logging.info(f"Downloading flux for : {year_month}")
        ObjectStorageClient().get_files(
            list_files=[
                File(
                    source_path=FLUX_SIRENE_CONFIG.object_storage_path,
                    source_name=f"flux_unite_legale_{year_month}.csv.gz",
                    dest_path=f"{data_dir}",
                    dest_name=f"flux_unite_legale_{year_month}.csv.gz",
                    content_type=None,
                )
            ],
        )
        df_iterator = pd.read_csv(
            f"{data_dir}flux_unite_legale_{year_month}.csv.gz",
            chunksize=100000,
            dtype=str,
            compression="gzip",
        )
        return df_iterator
    except ClientError as e:
        logging.warning(f"No flux data has been found for: {year_month}")
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise AirflowSkipException("Skipping this task")


def preprocess_unite_legale_data(data_dir, sirene_file_type):
    if sirene_file_type == "stock":
        df_iterator = download_stock(data_dir)
    if sirene_file_type == "flux":
        df_iterator = download_flux(data_dir)

    # Insert rows in database by chunk
    for _, df_unite_legale in enumerate(df_iterator):
        df_unite_legale = df_unite_legale[
            [
                "siren",
                "dateCreationUniteLegale",
                "sigleUniteLegale",
                "prenomUsuelUniteLegale",
                "identifiantAssociationUniteLegale",
                "trancheEffectifsUniteLegale",
                "dateDernierTraitementUniteLegale",
                "categorieEntreprise",
                "etatAdministratifUniteLegale",
                "nomUniteLegale",
                "nomUsageUniteLegale",
                "denominationUniteLegale",
                "denominationUsuelle1UniteLegale",
                "denominationUsuelle2UniteLegale",
                "denominationUsuelle3UniteLegale",
                "categorieJuridiqueUniteLegale",
                "activitePrincipaleUniteLegale",
                "activitePrincipaleNAF25UniteLegale",
                "economieSocialeSolidaireUniteLegale",
                "statutDiffusionUniteLegale",
                "societeMissionUniteLegale",
                "anneeCategorieEntreprise",
                "anneeEffectifsUniteLegale",
                "caractereEmployeurUniteLegale",
            ]
        ]
        # Rename columns
        df_unite_legale = df_unite_legale.rename(
            columns={
                "dateCreationUniteLegale": "date_creation_unite_legale",
                "sigleUniteLegale": "sigle",
                "prenomUsuelUniteLegale": "prenom",
                "trancheEffectifsUniteLegale": "tranche_effectif_salarie_unite_legale",
                "dateDernierTraitementUniteLegale": "date_mise_a_jour_insee",
                "categorieEntreprise": "categorie_entreprise",
                "etatAdministratifUniteLegale": "etat_administratif_unite_legale",
                "nomUniteLegale": "nom",
                "nomUsageUniteLegale": "nom_usage",
                "denominationUniteLegale": "nom_raison_sociale",
                "denominationUsuelle1UniteLegale": "denomination_usuelle_1",
                "denominationUsuelle2UniteLegale": "denomination_usuelle_2",
                "denominationUsuelle3UniteLegale": "denomination_usuelle_3",
                "categorieJuridiqueUniteLegale": "nature_juridique_unite_legale",
                "activitePrincipaleUniteLegale": "activite_principale_unite_legale",
                "activitePrincipaleNAF25UniteLegale": "activite_principale_naf25_unite_legale",
                "economieSocialeSolidaireUniteLegale": "economie_sociale_solidaire"
                "_unite_legale",
                "identifiantAssociationUniteLegale": "identifiant_association"
                "_unite_legale",
                "statutDiffusionUniteLegale": "statut_diffusion_unite_legale",
                "societeMissionUniteLegale": "est_societe_mission",
                "anneeCategorieEntreprise": "annee_categorie_entreprise",
                "anneeEffectifsUniteLegale": "annee_tranche_effectif_salarie",
                "caractereEmployeurUniteLegale": "caractere_employeur",
            }
        )
        df_unite_legale["from_insee"] = True
        yield df_unite_legale


def preprocess_historique_unite_legale_data(data_dir):
    df_iterator = download_historique(data_dir)

    # Insert rows in database by chunk
    for _, df_unite_legale in enumerate(df_iterator):
        df_unite_legale = df_unite_legale[
            [
                "siren",
                "dateFin",
                "dateDebut",
                "etatAdministratifUniteLegale",
                "changementEtatAdministratifUniteLegale",
                "nicSiegeUniteLegale",
                "changementNicSiegeUniteLegale",
            ]
        ]
        # Rename columns
        df_unite_legale = df_unite_legale.rename(
            columns={
                "dateFin": "date_fin_periode",
                "dateDebut": "date_debut_periode",
                "changementEtatAdministratifUniteLegale": "changement_etat"
                "_administratif_unite_legale",
                "etatAdministratifUniteLegale": "etat_administratif_unite_legale",
                "nicSiegeUniteLegale": "nic_siege",
                "changementNicSiegeUniteLegale": "changement_nic_siege_unite_legale",
            }
        )

        # Compute siege_siret directly in historique table
        df_unite_legale["siege_siret"] = (
            df_unite_legale["siren"] + df_unite_legale["nic_siege"]
        )

        yield df_unite_legale


def download_flux_periodes_unite_legale(data_dir):
    """Download flux periodes unite legale data from object storage."""
    year_month = get_sirene_processing_month()
    try:
        ObjectStorageClient().get_files(
            list_files=[
                File(
                    source_path=FLUX_SIRENE_CONFIG.object_storage_path,
                    source_name=f"flux_unite_legale_periodes_{year_month}.csv.gz",
                    dest_path=f"{data_dir}",
                    dest_name=f"flux_unite_legale_periodes_{year_month}.csv.gz",
                    content_type=None,
                )
            ],
        )
        df_flux_periodes = pd.read_csv(
            f"{data_dir}flux_unite_legale_periodes_{year_month}.csv.gz",
            compression="gzip",
            dtype=str,
        )
        return df_flux_periodes
    except ClientError as e:
        logging.warning(
            f"No flux periodes unite legale data has been found for: {year_month}"
        )
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise AirflowSkipException(
                "Skipping flux periodes unite legale - no data available"
            )
        raise


def preprocess_flux_periodes_unite_legale_data(data_dir):
    """Preprocess flux periodes unite legale data."""
    try:
        df_flux_periodes = download_flux_periodes_unite_legale(data_dir)
        df_flux_periodes = df_flux_periodes[
            [
                "siren",
                "dateFin",
                "dateDebut",
                "etatAdministratifUniteLegale",
                "changementEtatAdministratifUniteLegale",
                "nicSiegeUniteLegale",
                "changementNicSiegeUniteLegale",
            ]
        ]
        # Rename columns to match historique format
        df_flux_periodes = df_flux_periodes.rename(
            columns={
                "dateFin": "date_fin_periode",
                "dateDebut": "date_debut_periode",
                "changementEtatAdministratifUniteLegale": "changement_etat"
                "_administratif_unite_legale",
                "etatAdministratifUniteLegale": "etat_administratif_unite_legale",
                "nicSiegeUniteLegale": "nic_siege",
                "changementNicSiegeUniteLegale": "changement_nic_siege_unite_legale",
            }
        )
        # Calculate siege_siret
        df_flux_periodes["siege_siret"] = (
            df_flux_periodes["siren"] + df_flux_periodes["nic_siege"]
        )
        return df_flux_periodes
    except AirflowSkipException:
        # Return empty DataFrame if no flux periodes available
        return pd.DataFrame(
            columns=[
                "siren",
                "date_fin_periode",
                "date_debut_periode",
                "etat_administratif_unite_legale",
                "changement_etat_administratif_unite_legale",
                "nic_siege",
                "siege_siret",
                "changement_nic_siege_unite_legale",
            ]
        )
