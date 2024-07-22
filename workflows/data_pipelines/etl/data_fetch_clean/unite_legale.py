from datetime import datetime, timedelta
import shutil
import logging
import pandas as pd
import requests
from dag_datalake_sirene.helpers.s3_helpers import s3_client
from dag_datalake_sirene.config import (
    URL_MINIO_UNITE_LEGALE,
    URL_MINIO_UNITE_LEGALE_HISTORIQUE,
)


def download_historique(data_dir):
    r = requests.get(URL_MINIO_UNITE_LEGALE_HISTORIQUE, allow_redirects=True)
    open(data_dir + "StockUniteLegaleHistorique_utf8.zip", "wb").write(r.content)
    shutil.unpack_archive(data_dir + "StockUniteLegaleHistorique_utf8.zip", data_dir)
    df_iterator = pd.read_csv(
        f"{data_dir}StockUniteLegaleHistorique_utf8.csv",
        chunksize=100000,
        dtype=str,
    )
    return df_iterator


def download_stock(data_dir):
    r = requests.get(URL_MINIO_UNITE_LEGALE, allow_redirects=True)
    open(data_dir + "StockUniteLegale_utf8.zip", "wb").write(r.content)
    shutil.unpack_archive(data_dir + "StockUniteLegale_utf8.zip", data_dir)
    df_iterator = pd.read_csv(
        f"{data_dir}StockUniteLegale_utf8.csv", chunksize=100000, dtype=str
    )
    return df_iterator


def download_flux(data_dir):
    # If first of the month, use previous month data
    today = datetime.today()
    if today.day == 1:
        # Calculate the first day of the previous month
        first_day_of_previous_month = today - timedelta(days=1)
        year_month = first_day_of_previous_month.strftime("%Y-%m")
    else:
        year_month = datetime.today().strftime("%Y-%m")
    logging.info(f"Downloading flux for : {year_month}")
    s3_client.get_files(
        list_files=[
            {
                "source_path": "insee/sirene/flux/",
                "source_name": f"flux_unite_legale_{year_month}.csv.gz",
                "dest_path": f"{data_dir}",
                "dest_name": f"flux_unite_legale_{year_month}.csv.gz",
            }
        ],
    )
    df_iterator = pd.read_csv(
        f"{data_dir}flux_unite_legale_{year_month}.csv.gz",
        chunksize=100000,
        dtype=str,
        compression="gzip",
    )
    return df_iterator


def preprocess_unite_legale_data(data_dir, sirene_file_type):
    if sirene_file_type == "stock":
        df_iterator = download_stock(data_dir)
    if sirene_file_type == "flux":
        df_iterator = download_flux(data_dir)

    # Insert rows in database by chunk
    for i, df_unite_legale in enumerate(df_iterator):
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
    for i, df_unite_legale in enumerate(df_iterator):
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
                "nicSiegeUniteLegale": "nic_siege_unite_legale",
                "changementNicSiegeUniteLegale": "changement_nic_siege_unite_legale",
            }
        )
        yield df_unite_legale
