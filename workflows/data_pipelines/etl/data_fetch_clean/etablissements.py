import logging
import shutil
from typing import Literal

import pandas as pd
import requests
from airflow.exceptions import AirflowSkipException
from botocore.exceptions import ClientError

from data_pipelines_annuaire.config import (
    CURRENT_MONTH,
    PREVIOUS_MONTH,
)
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

STOCK_ETABLISSEMENT_COLUMNS = [
    "siren",
    "siret",
    "dateCreationEtablissement",
    "trancheEffectifsEtablissement",
    "caractereEmployeurEtablissement",
    "anneeEffectifsEtablissement",
    "dateDernierTraitementEtablissement",
    "activitePrincipaleRegistreMetiersEtablissement",
    "etablissementSiege",
    "numeroVoieEtablissement",
    "dernierNumeroVoieEtablissement",
    "libelleVoieEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    "libelleCedexEtablissement",
    "typeVoieEtablissement",
    "codeCommuneEtablissement",
    "codeCedexEtablissement",
    "complementAdresseEtablissement",
    "distributionSpecialeEtablissement",
    "dateDebut",
    "etatAdministratifEtablissement",
    "enseigne1Etablissement",
    "enseigne2Etablissement",
    "enseigne3Etablissement",
    "denominationUsuelleEtablissement",
    "activitePrincipaleEtablissement",
    "activitePrincipaleNAF25Etablissement",
    "indiceRepetitionEtablissement",
    "libelleCommuneEtrangerEtablissement",
    "codePaysEtrangerEtablissement",
    "libellePaysEtrangerEtablissement",
    "statutDiffusionEtablissement",
    "coordonneeLambertAbscisseEtablissement",
    "coordonneeLambertOrdonneeEtablissement",
    "latitude",
    "longitude",
]


def download_stock_iterator(chunksize: int = 500_000) -> pd.DataFrame:
    year_month = get_sirene_processing_month()
    logging.info(f"Downloading stock etablissement for: {year_month}")

    url = f"{STOCK_SIRENE_CONFIG.url_object_storage}StockEtablissement_{year_month}_with_gps.zip"
    logging.info(f"Stock file url: {url}")

    return pd.read_csv(
        url,
        compression="zip",
        dtype=str,
        usecols=STOCK_ETABLISSEMENT_COLUMNS,
        chunksize=chunksize,
    )


FLUX_ETABLISSEMENT_COLUMNS = [
    "siren",
    "siret",
    "dateCreationEtablissement",
    "trancheEffectifsEtablissement",
    "caractereEmployeurEtablissement",
    "anneeEffectifsEtablissement",
    "dateDernierTraitementEtablissement",
    "activitePrincipaleRegistreMetiersEtablissement",
    "etablissementSiege",
    "numeroVoieEtablissement",
    "dernierNumeroVoieEtablissement",
    "libelleVoieEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    "libelleCedexEtablissement",
    "typeVoieEtablissement",
    "codeCommuneEtablissement",
    "codeCedexEtablissement",
    "complementAdresseEtablissement",
    "distributionSpecialeEtablissement",
    "dateDebut",
    "etatAdministratifEtablissement",
    "enseigne1Etablissement",
    "enseigne2Etablissement",
    "enseigne3Etablissement",
    "denominationUsuelleEtablissement",
    "activitePrincipaleEtablissement",
    "activitePrincipaleNAF25Etablissement",
    "indiceRepetitionEtablissement",
    "libelleCommuneEtrangerEtablissement",
    "codePaysEtrangerEtablissement",
    "libellePaysEtrangerEtablissement",
    "statutDiffusionEtablissement",
    "coordonneeLambertAbscisseEtablissement",
    "coordonneeLambertOrdonneeEtablissement",
    "latitude",
    "longitude",
]


def download_flux_iterator(chunksize: int = 500_000) -> pd.DataFrame:
    year_month = get_sirene_processing_month()
    logging.info(f"Downloading flux for : {year_month}")
    try:
        url = f"{FLUX_SIRENE_CONFIG.url_object_storage}flux_etablissement_{year_month}.csv.gz"
        logging.info(f"Flux file url: {url}")
        return pd.read_csv(
            url,
            dtype=str,
            compression="gzip",
            usecols=FLUX_ETABLISSEMENT_COLUMNS,
            chunksize=chunksize,
        )
    # At the start of each month, a new stock file is published on data.gouv.
    # API Sirene returns an error when trying to get flux data for the new month
    # which means it not yet available on object storage.
    # This exception handling ensures that we skip downloading the flux data
    # if it hasn't been uploaded to object storage yet.
    except ClientError as e:
        logging.warning(f"No flux data has been found for: {year_month}")
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise AirflowSkipException("Skipping this task")


def download_historique(data_dir):
    year_month = get_sirene_processing_month()
    filename = STOCK_SIRENE_CONFIG.files_to_download["historique_etablissement"][
        "destination"
    ].split("/")[-1]
    filename = filename.replace(CURRENT_MONTH, year_month)
    url = STOCK_SIRENE_CONFIG.url_object_storage + filename

    r = requests.get(
        url,
        allow_redirects=True,
    )
    open(data_dir + "StockEtablissementHistorique_utf8.zip", "wb").write(r.content)
    shutil.unpack_archive(data_dir + "StockEtablissementHistorique_utf8.zip", data_dir)
    df_iterator = pd.read_csv(
        f"{data_dir}StockEtablissementHistorique_utf8.csv",
        chunksize=100000,
        dtype=str,
    )
    return df_iterator


def combine_numero_voie(numero_voie, dernier_numero_voie) -> str | None:
    if not pd.isna(dernier_numero_voie) and not pd.isna(numero_voie):
        return f"{numero_voie}-{dernier_numero_voie}"
    if not pd.isna(numero_voie):
        return numero_voie
    if not pd.isna(dernier_numero_voie):
        return dernier_numero_voie
    return None


ETABLISSEMENT_RENAME_COLUMNS = {
    "dateCreationEtablissement": "date_creation",
    "trancheEffectifsEtablissement": "tranche_effectif_salarie",
    "caractereEmployeurEtablissement": "caractere_employeur",
    "anneeEffectifsEtablissement": "annee_tranche_effectif_salarie",
    "dateDernierTraitementEtablissement": "date_mise_a_jour_insee",
    "activitePrincipaleRegistreMetiersEtablissement": "activite_principale_registre_metier",
    "etablissementSiege": "est_siege",
    "numeroVoieEtablissement": "numero_voie",
    "dernierNumeroVoieEtablissement": "dernier_numero_voie",
    "typeVoieEtablissement": "type_voie",
    "libelleVoieEtablissement": "libelle_voie",
    "codePostalEtablissement": "code_postal",
    "libelleCedexEtablissement": "libelle_cedex",
    "libelleCommuneEtablissement": "libelle_commune",
    "codeCommuneEtablissement": "commune",
    "complementAdresseEtablissement": "complement_adresse",
    "codeCedexEtablissement": "cedex",
    "dateDebut": "date_debut_activite",
    "distributionSpecialeEtablissement": "distribution_speciale",
    "etatAdministratifEtablissement": "etat_administratif_etablissement",
    "enseigne1Etablissement": "enseigne_1",
    "enseigne2Etablissement": "enseigne_2",
    "enseigne3Etablissement": "enseigne_3",
    "activitePrincipaleEtablissement": "activite_principale",
    "activitePrincipaleNAF25Etablissement": "activite_principale_naf25",
    "indiceRepetitionEtablissement": "indice_repetition",
    "denominationUsuelleEtablissement": "nom_commercial",
    "libelleCommuneEtrangerEtablissement": "libelle_commune_etranger",
    "codePaysEtrangerEtablissement": "code_pays_etranger",
    "libellePaysEtrangerEtablissement": "libelle_pays_etranger",
    "statutDiffusionEtablissement": "statut_diffusion_etablissement",
    "coordonneeLambertAbscisseEtablissement": "x",
    "coordonneeLambertOrdonneeEtablissement": "y",
}


def preprocess_etablissement_data(file_type: Literal["stock", "flux"]):
    if file_type == "stock":
        download_iterator = download_stock_iterator
    elif file_type == "flux":
        download_iterator = download_flux_iterator

    for df_chunk in download_iterator():
        df_chunk["etablissementSiege"] = df_chunk["etablissementSiege"].apply(
            lambda x: x.lower()
        )
        df_chunk = df_chunk.rename(columns=ETABLISSEMENT_RENAME_COLUMNS)
        df_chunk["coord_source"] = df_chunk.apply(
            lambda row: (
                file_type
                if pd.notna(row["latitude"]) and pd.notna(row["longitude"])
                else f"{file_type}_vide"
            ),
            axis=1,
        )
        yield df_chunk


def preprocess_historique_etablissement_data(data_dir):
    df_iterator = download_historique(data_dir)

    # Insert rows in database by chunk
    for _, df_etablissement in enumerate(df_iterator):
        df_etablissement = df_etablissement[
            [
                "siren",
                "siret",
                "dateFin",
                "dateDebut",
                "etatAdministratifEtablissement",
                "changementEtatAdministratifEtablissement",
            ]
        ]
        # Rename columns
        df_etablissement = df_etablissement.rename(
            columns={
                "dateFin": "date_fin_periode",
                "dateDebut": "date_debut_periode",
                "changementEtatAdministratifEtablissement": "changement_etat_administratif_etablissement",
                "etatAdministratifEtablissement": "etat_administratif_etablissement",
            }
        )
        yield df_etablissement


def download_flux_periodes(data_dir):
    """Download flux periodes data from object storage."""
    year_month = get_sirene_processing_month()
    try:
        ObjectStorageClient().get_files(
            list_files=[
                File(
                    source_path=FLUX_SIRENE_CONFIG.object_storage_path,
                    source_name=f"flux_etablissement_periodes_{year_month}.csv.gz",
                    dest_path=f"{data_dir}",
                    dest_name=f"flux_etablissement_periodes_{year_month}.csv.gz",
                    content_type=None,
                )
            ],
        )
        df_flux_periodes = pd.read_csv(
            f"{data_dir}flux_etablissement_periodes_{year_month}.csv.gz",
            compression="gzip",
            dtype=str,
        )
        return df_flux_periodes
    except ClientError as e:
        logging.warning(f"No flux periodes data has been found for: {year_month}")
        if e.response["Error"]["Code"] == "NoSuchKey":
            raise AirflowSkipException("Skipping flux periodes - no data available")
        raise


def preprocess_flux_periodes_data(data_dir):
    """Preprocess flux periodes data for date_fermeture calculation."""
    try:
        df_flux_periodes = download_flux_periodes(data_dir)
        # Rename columns to match historique format
        df_flux_periodes = df_flux_periodes.rename(
            columns={
                "dateFin": "date_fin_periode",
                "dateDebut": "date_debut_periode",
                "changementEtatAdministratifEtablissement": "changement_etat_administratif_etablissement",
                "etatAdministratifEtablissement": "etat_administratif_etablissement",
            }
        )
        return df_flux_periodes
    except AirflowSkipException:
        # Return empty DataFrame if no flux periodes available
        return pd.DataFrame(
            columns=[
                "siren",
                "siret",
                "date_fin_periode",
                "date_debut_periode",
                "changement_etat_administratif_etablissement",
                "etat_administratif_etablissement",
            ]
        )


def download_geo_stats_iterator(data_dir: str, chunksize: int = 500_000):
    filename = STOCK_SIRENE_CONFIG.files_to_download["geo_stats"]["destination"].split(
        "/"
    )[-1]
    url = STOCK_SIRENE_CONFIG.url_object_storage + filename

    logging.info(f"Downloading and unpacking {url}..")
    try:
        r = requests.get(
            url,
            allow_redirects=True,
        )
        # Check if the response indicates a NoSuchKey error
        if r.status_code == 404:
            # Try with previous month's file
            filename_prev = filename.replace(CURRENT_MONTH, PREVIOUS_MONTH)
            url_prev = STOCK_SIRENE_CONFIG.url_object_storage + filename_prev
            logging.warning(
                f"File not found at {url}, trying previous month: {url_prev}"
            )
            r = requests.get(
                url_prev,
                allow_redirects=True,
            )
            filename = filename_prev

        open(data_dir + filename, "wb").write(r.content)
        shutil.unpack_archive(data_dir + filename, data_dir)

        return pd.read_csv(
            f"{data_dir}GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.csv",
            sep=";",
            dtype=str,
            usecols=["siret", "x_longitude", "y_latitude"],
            chunksize=chunksize,
        )
    except Exception as e:
        logging.error(f"Failed to download geo stats: {e}")
        raise


def preprocess_geo_stats_data(data_dir: str):
    for df_chunk in download_geo_stats_iterator(data_dir):
        df_chunk = df_chunk.rename(
            columns={
                "x_longitude": "longitude",
                "y_latitude": "latitude",
            }
        )
        df_chunk["latitude"] = df_chunk["latitude"].replace("[ND]", None)
        df_chunk["longitude"] = df_chunk["longitude"].replace("[ND]", None)
        yield df_chunk
