import ast
import logging
import shutil

import minio
import pandas as pd
import requests
from airflow.exceptions import AirflowSkipException

from dag_datalake_sirene.config import CURRENT_MONTH
from dag_datalake_sirene.helpers.minio_helpers import File, MinIOClient
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.determine_sirene_date import (
    get_sirene_processing_month,
)
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.config import (
    FLUX_SIRENE_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.sirene.stock.config import (
    STOCK_SIRENE_CONFIG,
)


def download_historique(data_dir):
    year_month = get_sirene_processing_month()
    filename = STOCK_SIRENE_CONFIG.files_to_download["historique_unite_legale"][
        "destination"
    ].split("/")[-1]
    filename = filename.replace(CURRENT_MONTH, year_month)
    url = STOCK_SIRENE_CONFIG.url_minio + filename

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
    url = STOCK_SIRENE_CONFIG.url_minio + filename

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
        MinIOClient().get_files(
            list_files=[
                File(
                    source_path=FLUX_SIRENE_CONFIG.minio_path,
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
    except minio.error.S3Error as e:
        logging.warning(f"No flux data has been found for: {year_month}")
        if e.code == "NoSuchKey":
            raise AirflowSkipException("Skipping this task")


def extract_nic_list(periods_data):
    nic_list = []
    for row in ast.literal_eval(periods_data):
        nic_value = row["nicSiegeUniteLegale"]
        if nic_value is not None:
            nic_list.append(nic_value)
    return list(set(nic_list))


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

        # Create a new DataFrame for SIREN-NIC relationships
        siren_nic_df = df_unite_legale[["siren", "nic_siege"]].drop_duplicates()
        siren_nic_df = siren_nic_df.astype({"siren": str, "nic_siege": str})

        # Create the new column combining SIREN and NIC
        siren_nic_df["siret"] = siren_nic_df["siren"] + siren_nic_df["nic_siege"]

        yield df_unite_legale, siren_nic_df


def process_ancien_siege_flux(data_dir):
    """
    The function uses the 'extract_nic_list' function to extract the NIC from the
    'periodesUniteLegale' column. It then explodes the resulting
    DataFrame to create a row for each NIC, removes duplicates, and casts the 'siren'
    and 'nic_siege' columns to strings before computing the 'siret' column.

    The function is a generator, yielding the resulting DataFrame in chunks.
    """
    df_iterator = download_flux(data_dir)

    for _, df_unite_legale in enumerate(df_iterator):
        df_expanded = (
            df_unite_legale[["siren", "periodesUniteLegale"]]
            .assign(
                nic_siege=df_unite_legale["periodesUniteLegale"].apply(extract_nic_list)
            )
            .drop("periodesUniteLegale", axis=1)
            .explode("nic_siege")
            .drop_duplicates(subset=["siren", "nic_siege"])
            .astype({"siren": str, "nic_siege": str})
            .assign(siret=lambda df: df["siren"] + df["nic_siege"])
        )
        yield df_expanded
