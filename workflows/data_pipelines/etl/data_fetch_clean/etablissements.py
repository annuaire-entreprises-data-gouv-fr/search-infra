import logging
import shutil

import minio
import pandas as pd
import requests
from airflow.exceptions import AirflowSkipException

from dag_datalake_sirene.config import (
    CURRENT_MONTH,
    PREVIOUS_MONTH,
    URL_STOCK_ETABLISSEMENTS,
)
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


def download_stock(departement):
    year_month = get_sirene_processing_month()
    if year_month == CURRENT_MONTH:
        stock_version = "current"
    elif year_month == PREVIOUS_MONTH:
        stock_version = "previous"
    else:
        raise NotImplementedError("No stock to download")

    url = f"{URL_STOCK_ETABLISSEMENTS[stock_version]}_{departement}.csv.gz"
    logging.info(f"Département file url: {url}")
    df_dep = pd.read_csv(
        url,
        compression="gzip",
        dtype=str,
        usecols=[
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
            "enseigne1Etablissement",
            "enseigne2Etablissement",
            "enseigne3Etablissement",
            "denominationUsuelleEtablissement",
            "activitePrincipaleEtablissement",
            "indiceRepetitionEtablissement",
            "libelleCommuneEtrangerEtablissement",
            "codePaysEtrangerEtablissement",
            "libellePaysEtrangerEtablissement",
            "statutDiffusionEtablissement",
            "coordonneeLambertAbscisseEtablissement",
            "coordonneeLambertOrdonneeEtablissement",
            "longitude",
            "latitude",
            "geo_adresse",
            "geo_id",
            "geo_score",
        ],
    )
    return df_dep


def download_flux(data_dir):
    year_month = get_sirene_processing_month()
    logging.info(f"Downloading flux for : {year_month}")
    try:
        MinIOClient().get_files(
            list_files=[
                File(
                    source_path=FLUX_SIRENE_CONFIG.minio_path,
                    source_name=f"flux_etablissement_{year_month}.csv.gz",
                    dest_path=f"{data_dir}",
                    dest_name=f"flux_etablissement_{year_month}.csv.gz",
                    content_type=None,
                )
            ],
        )
        df_flux = pd.read_csv(
            f"{data_dir}flux_etablissement_{year_month}.csv.gz",
            dtype=str,
            compression="gzip",
            usecols=[
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
                "enseigne1Etablissement",
                "enseigne2Etablissement",
                "enseigne3Etablissement",
                "denominationUsuelleEtablissement",
                "activitePrincipaleEtablissement",
                "indiceRepetitionEtablissement",
                "libelleCommuneEtrangerEtablissement",
                "codePaysEtrangerEtablissement",
                "libellePaysEtrangerEtablissement",
                "statutDiffusionEtablissement",
                "coordonneeLambertAbscisseEtablissement",
                "coordonneeLambertOrdonneeEtablissement",
            ],
        )
        return df_flux
    # At the start of each month, a new stock file is published on data.gouv.
    # API Sirene returns an error when trying to get flux data for the new month
    # which means it not yet available on MinIO.
    # This exception handling ensures that we skip downloading the flux data
    # if it hasn't been uploaded to MinIO yet.
    except minio.error.S3Error as e:
        logging.warning(f"No flux data has been found for: {year_month}")
        if e.code == "NoSuchKey":
            raise AirflowSkipException("Skipping this task")


def download_historique(data_dir):
    year_month = get_sirene_processing_month()
    filename = STOCK_SIRENE_CONFIG.files_to_download["historique_etablissement"][
        "destination"
    ].split("/")[-1]
    filename = filename.replace(CURRENT_MONTH, year_month)
    url = STOCK_SIRENE_CONFIG.url_minio + filename

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


def preprocess_etablissement_data(siret_file_type, departement=None, data_dir=None):
    if siret_file_type == "stock":
        df_etablissement = download_stock(departement)
    elif siret_file_type == "flux":
        df_etablissement = download_flux(data_dir)
    else:
        raise NotImplementedError("Only stock and flux are implemented")

    # Insert rows in database
    df_etablissement["etablissementSiege"] = df_etablissement[
        "etablissementSiege"
    ].apply(lambda x: x.lower())
    df_etablissement = df_etablissement.rename(
        columns={
            "dateCreationEtablissement": "date_creation",
            "trancheEffectifsEtablissement": "tranche_effectif_salarie",
            "caractereEmployeurEtablissement": "caractere_employeur",
            "anneeEffectifsEtablissement": "annee_tranche_effectif_salarie",
            "dateDernierTraitementEtablissement": "date_mise_a_jour_insee",
            "activitePrincipaleRegistreMetiersEtablissement": "activite_principale"
            "_registre_metier",
            "etablissementSiege": "est_siege",
            "numeroVoieEtablissement": "numero_voie",
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
            "indiceRepetitionEtablissement": "indice_repetition",
            "denominationUsuelleEtablissement": "nom_commercial",
            "libelleCommuneEtrangerEtablissement": "libelle_commune_etranger",
            "codePaysEtrangerEtablissement": "code_pays_etranger",
            "libellePaysEtrangerEtablissement": "libelle_pays_etranger",
            "statutDiffusionEtablissement": "statut_diffusion_etablissement",
            "coordonneeLambertAbscisseEtablissement": "x",
            "coordonneeLambertOrdonneeEtablissement": "y",
        }
    )
    return df_etablissement


def preprocess_historique_etablissement_data(data_dir):
    df_iterator = download_historique(data_dir)

    # Insert rows in database by chunk
    for i, df_etablissement in enumerate(df_iterator):
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
                "changementEtatAdministratifEtablissement": "changement_etat"
                "_administratif_etablissement",
                "etatAdministratifEtablissement": "etat_administratif_etablissement",
            }
        )
        yield df_etablissement
