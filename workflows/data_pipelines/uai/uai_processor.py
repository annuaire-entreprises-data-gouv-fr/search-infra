import pandas as pd

from dag_datalake_sirene.helpers import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.uai.uai_config import (
    UAI_CONFIG,
)


class UAIProcessor(DataProcessor):
    def __init__(self):
        super().__init__(UAI_CONFIG)

    def preprocess_data(self):
        target_columns = [
            "uai",
            "denomination",
            "sigle",
            "adresse",
            "code_postal",
            "code_commune",
            "commune",
            "siren",
            "siret",
            "public_prive",
            "statut_prive",
            "type",
        ]

        # menj
        df_menj = pd.read_csv(
            self.config.files_to_download["menj"]["destination"],
            dtype=str,
            sep=";",
            encoding="Latin-1",
        )
        df_menj = df_menj.rename(
            columns={
                "Identifiant_de_l_etablissement": "uai",
                "Nom_etablissement": "denomination",
                "Adresse_1": "adresse",
                "Code_postal": "code_postal",
                "Code_commune": "code_commune",
                "Nom_commune": "commune",
                "SIREN_SIRET": "siret",
                "Statut_public_prive": "public_prive",
                "Type_contrat_prive": "statut_prive",
                "Type_etablissement": "type",
            }
        )
        df_menj["sigle"] = None
        df_menj["siren"] = df_menj["siret"].str[:9]
        df_menj = df_menj[target_columns]

        # mesr
        df_mesr = pd.read_csv(
            self.config.files_to_download["mesr"]["destination"], dtype=str, sep=";"
        )
        df_mesr = df_mesr.rename(
            columns={
                "uai - identifiant": "uai",
                "uo_lib_officiel": "denomination",
                "sigle": "sigle",
                "Adresse": "adresse",
                "Code postal": "code_postal",
                "Code commune": "code_commune",
                "Commune": "commune",
                "siren": "siren",
                "siret": "siret",
                "secteur d'établissement": "public_prive",
                "type d'établissement": "type",
            }
        )
        df_mesr["statut_prive"] = None
        df_mesr = df_mesr[target_columns]

        # onisep
        df_onisep = pd.read_csv(
            self.config.files_to_download["onisep"]["destination"], dtype=str, sep=";"
        )
        df_onisep = df_onisep.rename(
            columns={
                "code UAI": "uai",
                "nom": "denomination",
                "sigle": "sigle",
                "adresse": "adresse",
                "CP": "code_postal",
                "commune (COG)": "code_commune",
                "commune": "commune",
                "n° SIRET": "siret",
                "statut": "public_prive",
                "type d'établissement": "type",
            }
        )
        df_onisep["siren"] = df_onisep["siret"].str[:9]
        df_onisep["statut_prive"] = None
        df_onisep = df_onisep[target_columns]

        # output
        annuaire_uai = pd.concat([df_menj, df_mesr, df_onisep], ignore_index=True)
        annuaire_uai = annuaire_uai.drop_duplicates(subset=["uai"], keep="first")
        annuaire_uai.to_csv(self.config.file_output, index=False)

        self._push_unique_count(
            annuaire_uai["uai"], Notification.notification_xcom_key, "UAI"
        )
        self._push_unique_count(
            annuaire_uai["siret"], Notification.notification_xcom_key, "siret"
        )

        del df_onisep
        del df_menj
        del df_mesr
