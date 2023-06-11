from datetime import datetime
import shutil

import pandas as pd
import requests

from dag_datalake_sirene.task_functions.get_and_put_minio_object import (
    get_object_minio,
)
from dag_datalake_sirene.task_functions.global_variables import (
    MINIO_BUCKET_DATA_PIPELINE,
)


def download_stock(data_dir):
    url = "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip"
    r = requests.get(url, allow_redirects=True)
    open(data_dir + "StockUniteLegale_utf8.zip", "wb").write(r.content)
    shutil.unpack_archive(data_dir + "StockUniteLegale_utf8.zip", data_dir)
    df_iterator = pd.read_csv(
        f"{data_dir}StockUniteLegale_utf8.csv", chunksize=100000, dtype=str
    )
    return df_iterator


def download_flux(data_dir):
    year_month = datetime.today().strftime("%Y-%m")
    get_object_minio(
        f"flux_unite_legale_{year_month}.csv.gz",
        "prod/insee/sirene/sirene_flux/",
        f"{data_dir}flux_unite_legale_{year_month}.csv.gz",
        MINIO_BUCKET_DATA_PIPELINE,
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
            ]
        ]
        # Rename columns
        df_unite_legale = df_unite_legale.rename(
            columns={
                "dateCreationUniteLegale": "date_creation_unite_legale",
                "sigleUniteLegale": "sigle",
                "prenomUsuelUniteLegale": "prenom",
                "trancheEffectifsUniteLegale": "tranche_effectif_salarie_unite_legale",
                "dateDernierTraitementUniteLegale": "date_mise_a_jour_unite_legale",
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
            }
        )
        yield df_unite_legale
