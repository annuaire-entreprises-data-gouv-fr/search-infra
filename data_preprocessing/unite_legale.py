import shutil

import pandas as pd
import requests


def preprocess_unite_legale_data(data_dir):
    url = "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip"
    r = requests.get(url, allow_redirects=True)
    open(data_dir + "StockUniteLegale_utf8.zip", "wb").write(r.content)
    shutil.unpack_archive(data_dir + "StockUniteLegale_utf8.zip", data_dir)
    df_iterator = pd.read_csv(
        data_dir + "StockUniteLegale_utf8.csv", chunksize=100000, dtype=str
    )
    # Insert rows in database by chunk
    for i, df_unite_legale in enumerate(df_iterator):
        df_unite_legale = df_unite_legale[
            [
                "siren",
                "dateCreationUniteLegale",
                "sigleUniteLegale",
                "prenom1UniteLegale",
                "identifiantAssociationUniteLegale",
                "trancheEffectifsUniteLegale",
                "dateDernierTraitementUniteLegale",
                "categorieEntreprise",
                "etatAdministratifUniteLegale",
                "nomUniteLegale",
                "nomUsageUniteLegale",
                "denominationUniteLegale",
                "categorieJuridiqueUniteLegale",
                "activitePrincipaleUniteLegale",
                "economieSocialeSolidaireUniteLegale",
            ]
        ]
        # Rename columns
        df_unite_legale = df_unite_legale.rename(
            columns={
                "dateCreationUniteLegale": "date_creation_unite_legale",
                "sigleUniteLegale": "sigle",
                "prenom1UniteLegale": "prenom",
                "trancheEffectifsUniteLegale": "tranche_effectif_salarie_unite_legale",
                "dateDernierTraitementUniteLegale": "date_mise_a_jour_unite_legale",
                "categorieEntreprise": "categorie_entreprise",
                "etatAdministratifUniteLegale": "etat_administratif_unite_legale",
                "nomUniteLegale": "nom",
                "nomUsageUniteLegale": "nom_usage",
                "denominationUniteLegale": "nom_raison_sociale",
                "categorieJuridiqueUniteLegale": "nature_juridique_unite_legale",
                "activitePrincipaleUniteLegale": "activite_principale_unite_legale",
                "economieSocialeSolidaireUniteLegale": "economie_sociale_solidaire"
                "_unite_legale",
                "identifiantAssociationUniteLegale": "identifiant_association"
                "_unite_legale",
            }
        )
        yield df_unite_legale
