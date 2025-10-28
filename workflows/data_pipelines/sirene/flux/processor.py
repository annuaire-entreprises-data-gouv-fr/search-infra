import logging
from datetime import datetime

import pandas as pd

from dag_datalake_sirene.helpers.data_processor import DataProcessor, Notification
from dag_datalake_sirene.helpers.minio_helpers import File
from dag_datalake_sirene.helpers.utils import (
    get_dates_since_start_of_month,
    zip_file,
)
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.api import (
    SireneApiClient,
)
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.config import (
    FLUX_SIRENE_CONFIG,
)


class SireneFluxProcessor(DataProcessor):
    BASE_UNITE_LEGALE_ENDPOINT = (
        "siren?q=dateDernierTraitementUniteLegale%3A{}&champs={}&nombre=1000"
    )
    BASE_ETABLISSEMENT_ENDPOINT = (
        "siret?q=dateDernierTraitementEtablissement%3A{}&champs={}&nombre=1000"
    )

    def __init__(self):
        super().__init__(FLUX_SIRENE_CONFIG)
        if not self.config.url_api:
            raise ValueError("Sirene API flux URL is missing!")
        if not self.config.auth_api:
            raise ValueError("Sirene API flux token is missing!")

        self.client = SireneApiClient(self.config.url_api, self.config.auth_api)
        self.current_month = datetime.today().strftime("%Y-%m")
        # Descending order so we start by the most recent day
        # Higher priority to the latest SIRENE flux updates
        self.current_dates = get_dates_since_start_of_month(
            include_today=False, ascending=False
        )

    @staticmethod
    def _construct_endpoint(base_endpoint: str, date: str, fields: str) -> str:
        return base_endpoint.format(date, fields)

    def _create_empty_csv_with_headers(self, fields: str, output_path: str) -> None:
        """Create an empty CSV file with headers only."""
        headers = fields.split(",")
        empty_df = pd.DataFrame(columns=headers)
        empty_df.to_csv(output_path, mode="w", header=True, index=False)
        logging.info(f"Created empty CSV with headers at {output_path}")

    def get_current_flux_unite_legale(self):
        fields = (
            "siren,"
            "dateCreationUniteLegale,"
            "sigleUniteLegale,"
            "prenomUsuelUniteLegale,"
            "identifiantAssociationUniteLegale,"
            "trancheEffectifsUniteLegale,"
            "dateDernierTraitementUniteLegale,"
            "categorieEntreprise,"
            "etatAdministratifUniteLegale,"
            "nomUniteLegale,"
            "nomUsageUniteLegale,"
            "denominationUniteLegale,"
            "denominationUsuelle1UniteLegale,"
            "denominationUsuelle2UniteLegale,"
            "denominationUsuelle3UniteLegale,"
            "categorieJuridiqueUniteLegale,"
            "activitePrincipaleUniteLegale,"
            "economieSocialeSolidaireUniteLegale,"
            "statutDiffusionUniteLegale,"
            "societeMissionUniteLegale,"
            "anneeCategorieEntreprise,"
            "anneeEffectifsUniteLegale,"
            "caractereEmployeurUniteLegale,"
            "nicSiegeUniteLegale"
        )
        output_path = (
            f"{self.config.tmp_folder}flux_unite_legale_{self.current_month}.csv"
        )

        if datetime.today().day == 1:
            logging.info(
                "First of the month, the flux API won't return any output. A headers only CSV is created instead."
            )
            self._create_empty_csv_with_headers(
                fields + ",periodesUniteLegale", output_path
            )
            zip_file(output_path)
            DataProcessor.push_message(
                Notification.notification_xcom_key,
                description="0 siren (empty file created)",
            )
            return

        siren_processed = pd.Series(dtype="string")

        logging.info(f"Processing the following dates: {self.current_dates}")
        for i_date, date in enumerate(self.current_dates):
            logging.info(f"{date} -- processing..")
            endpoint = self._construct_endpoint(
                self.BASE_UNITE_LEGALE_ENDPOINT, date, fields
            )
            df = self.client.fetch_data(endpoint, "unitesLegales")

            # Remove any SIREN we already got the last update from
            df = df[~df["siren"].isin(siren_processed)]
            siren_processed = pd.concat([siren_processed, df["siren"]])

            # Overwrite file with headers for the first dump, append only afterward
            if i_date == 0:
                df.to_csv(output_path, mode="w", header=True, index=False)
            else:
                df.to_csv(output_path, mode="a", header=False, index=False)

            logging.info(f"{date} -- processed: {df['siren'].nunique()} siren")
            del df

        n_siren_processed = len(siren_processed)
        logging.info(f"CSV output ready with {n_siren_processed} siren")
        zip_file(output_path)
        logging.info("File zipped. Done!")

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"{n_siren_processed} siren",
        )

    def get_current_flux_etablissement(self):
        fields = (
            "siren,"
            "siret,"
            "categorieJuridiqueUniteLegale,"
            "dateCreationEtablissement,"
            "trancheEffectifsEtablissement,"
            "activitePrincipaleRegistreMetiersEtablissement,"
            "etablissementSiege,"
            "anneeEffectifsEtablissement,"
            "libelleVoieEtablissement,"
            "codePostalEtablissement,"
            "numeroVoieEtablissement,"
            "dateDernierTraitementEtablissement,"
            "libelleCommuneEtablissement,"
            "libelleCedexEtablissement,"
            "typeVoieEtablissement,"
            "codeCommuneEtablissement,"
            "codeCedexEtablissement,"
            "complementAdresseEtablissement,"
            "distributionSpecialeEtablissement,"
            "dateDebut,"
            "etatAdministratifEtablissement,"
            "enseigne1Etablissement,"
            "enseigne2Etablissement,"
            "enseigne3Etablissement,"
            "denominationUsuelleEtablissement,"
            "activitePrincipaleEtablissement,"
            "indiceRepetitionEtablissement,"
            "libelleCommuneEtrangerEtablissement,"
            "codePaysEtrangerEtablissement,"
            "libellePaysEtrangerEtablissement,"
            "statutDiffusionEtablissement,"
            "caractereEmployeurEtablissement,"
            "coordonneeLambertAbscisseEtablissement,"
            "coordonneeLambertOrdonneeEtablissement"
        )
        output_path = (
            f"{self.config.tmp_folder}flux_etablissement_{self.current_month}.csv"
        )

        if datetime.today().day == 1:
            logging.info(
                "First day of the month, the flux API won't return any output. A headers only CSV is created instead."
            )
            self._create_empty_csv_with_headers(fields, output_path)
            zip_file(output_path)
            DataProcessor.push_message(
                Notification.notification_xcom_key,
                description="0 siret (empty file created)",
            )
            return

        siret_processed = pd.Series(dtype="string")

        logging.info(f"Processing the following dates: {self.current_dates}")
        for i_date, date in enumerate(self.current_dates):
            logging.info(f"{date} -- processing..")
            endpoint = self._construct_endpoint(
                self.BASE_ETABLISSEMENT_ENDPOINT, date, fields
            )
            df = self.client.fetch_data(endpoint, "etablissements")
            df.columns = [
                col.replace("adresseEtablissement_", "")
                if "adresseEtablissement_" in col
                else col
                for col in df.columns
            ]

            # Remove any SIRET we already got the last update from
            df = df[~df["siret"].isin(siret_processed)]
            siret_processed = pd.concat([siret_processed, df["siret"]])

            # Overwrite file with headers for the first dump, append only afterward
            if i_date == 0:
                df.to_csv(output_path, mode="w", header=True, index=False)
            else:
                df.to_csv(output_path, mode="a", header=False, index=False)

            logging.info(f"{date} -- processed: {df['siret'].nunique()} siret")
            del df

        n_siret_processed = len(siret_processed)
        logging.info(f"CSV output ready with {n_siret_processed} siret")
        zip_file(output_path)
        logging.info("File zipped. Done!")

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"{n_siret_processed} siret",
        )

    def send_flux_to_minio(self):
        self.minio_client.send_files(
            list_files=[
                File(
                    source_path=self.config.tmp_folder,
                    source_name=f"flux_unite_legale_{self.current_month}.csv.gz",
                    dest_path=self.config.minio_path,
                    dest_name=f"flux_unite_legale_{self.current_month}.csv.gz",
                    content_type=None,
                ),
                File(
                    source_path=self.config.tmp_folder,
                    source_name=f"flux_etablissement_{self.current_month}.csv.gz",
                    dest_path=self.config.minio_path,
                    dest_name=f"flux_etablissement_{self.current_month}.csv.gz",
                    content_type=None,
                ),
                File(
                    source_path=self.config.tmp_folder,
                    source_name="metadata.json",
                    dest_path=self.config.minio_path,
                    dest_name="metadata.json",
                    content_type=None,
                ),
            ],
        )
