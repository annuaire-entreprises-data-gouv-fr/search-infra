import logging
import pandas as pd
import requests
import shutil
from datetime import datetime, timedelta
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.config import (
    URL_ETABLISSEMENTS,
    URL_MINIO_ETABLISSEMENTS_HISTORIQUE,
)


def download_stock(departement):
    url = f"{URL_ETABLISSEMENTS}_{departement}.csv.gz"
    logging.info(f"Dep file url: {url}")
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
            "complementAdresse2Etablissement",
            "indiceRepetition2Etablissement",
            "libelleCedex2Etablissement",
            "codeCedex2Etablissement",
            "numeroVoie2Etablissement",
            "typeVoie2Etablissement",
            "libelleVoie2Etablissement",
            "codeCommune2Etablissement",
            "libelleCommune2Etablissement",
            "distributionSpeciale2Etablissement",
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
            "libelleCommuneEtranger2Etablissement",
            "codePaysEtranger2Etablissement",
            "libellePaysEtranger2Etablissement",
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
    today = datetime.today()
    if today.day == 1:
        # Calculate the first day of the previous month
        first_day_of_previous_month = today - timedelta(days=1)
        year_month = first_day_of_previous_month.strftime("%Y-%m")
    else:
        year_month = datetime.today().strftime("%Y-%m")
    logging.info(f"Downloading flux for : {year_month}")
    minio_client.get_files(
        list_files=[
            {
                "source_path": "insee/sirene/flux/",
                "source_name": f"flux_etablissement_{year_month}.csv.gz",
                "dest_path": f"{data_dir}",
                "dest_name": f"flux_etablissement_{year_month}.csv.gz",
            }
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
            "complementAdresse2Etablissement",
            "indiceRepetition2Etablissement",
            "libelleCedex2Etablissement",
            "codeCedex2Etablissement",
            "numeroVoie2Etablissement",
            "typeVoie2Etablissement",
            "libelleVoie2Etablissement",
            "codeCommune2Etablissement",
            "libelleCommune2Etablissement",
            "distributionSpeciale2Etablissement",
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
            "libelleCommuneEtranger2Etablissement",
            "codePaysEtranger2Etablissement",
            "libellePaysEtranger2Etablissement",
            "statutDiffusionEtablissement",
            "coordonneeLambertAbscisseEtablissement",
            "coordonneeLambertOrdonneeEtablissement",
        ],
    )
    return df_flux


def download_historique(data_dir):
    r = requests.get(URL_MINIO_ETABLISSEMENTS_HISTORIQUE, allow_redirects=True)
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
    if siret_file_type == "flux":
        df_etablissement = download_flux(data_dir)

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
            "complementAdresse2Etablissement": "complement_adresse_2",
            "numeroVoie2Etablissement": "numero_voie_2",
            "indiceRepetition2Etablissement": "indice_repetition_2",
            "typeVoie2Etablissement": "type_voie_2",
            "libelleVoie2Etablissement": "libelle_voie_2",
            "codeCommune2Etablissement": "commune_2",
            "libelleCommune2Etablissement": "libelle_commune_2",
            "codeCedex2Etablissement": "cedex_2",
            "libelleCedex2Etablissement": "libelle_cedex_2",
            "codeCedexEtablissement": "cedex",
            "dateDebut": "date_debut_activite",
            "distributionSpecialeEtablissement": "distribution_speciale",
            "distributionSpeciale2Etablissement": "distribution_speciale_2",
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
            "libelleCommuneEtranger2Etablissement": "libelle_commune_etranger_2",
            "codePaysEtranger2Etablissement": "code_pays_etranger_2",
            "libellePaysEtranger2Etablissement": "libelle_pays_etranger_2",
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
