import logging
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from data_pipelines_annuaire.helpers.data_processor import DataProcessor, Notification
from data_pipelines_annuaire.helpers.minio_helpers import File
from data_pipelines_annuaire.helpers.utils import (
    get_dates_since_start_of_month,
    zip_file,
)
from data_pipelines_annuaire.workflows.data_pipelines.sirene.flux.api import (
    SireneApiClient,
)
from data_pipelines_annuaire.workflows.data_pipelines.sirene.flux.config import (
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
            "activitePrincipaleNAF25UniteLegale,"
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
        for i_date, processing_date in enumerate(self.current_dates):
            logging.info(f"{processing_date} -- processing..")
            endpoint = self._construct_endpoint(
                self.BASE_UNITE_LEGALE_ENDPOINT, processing_date, fields
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

            logging.info(
                f"{processing_date} -- processed: {df['siren'].nunique()} siren"
            )
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
            "activitePrincipaleNAF25Etablissement,"
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
        for i_date, processing_date in enumerate(self.current_dates):
            logging.info(f"{processing_date} -- processing..")
            endpoint = self._construct_endpoint(
                self.BASE_ETABLISSEMENT_ENDPOINT, processing_date, fields
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

            logging.info(
                f"{processing_date} -- processed: {df['siret'].nunique()} siret"
            )
            del df

        n_siret_processed = len(siret_processed)
        logging.info(f"CSV output ready with {n_siret_processed} siret")
        zip_file(output_path)
        logging.info("File zipped. Done!")

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"{n_siret_processed} siret",
        )

    def check_updates_availability(self) -> str:
        """
        Check that INSEE API has today's data available.
        If not, wait 5 minutes and retry until it is today.
        If it's 9 AM or later and still not today, continue without waiting.

        Returns:
            str: Success message when data is available for today or when continuing at 9 AM
        """
        paris_tz = ZoneInfo("Europe/Paris")

        while True:
            try:
                # Get current time in Paris timezone
                current_time = datetime.now(paris_tz)
                today = current_time.date()
                max_wait_time = current_time.replace(
                    hour=9, minute=0, second=0, microsecond=0
                )

                # Check if we've reached 9 AM before making the API call (in Paris time)
                if current_time >= max_wait_time:
                    logging.info(
                        f"It's 9 AM or later. Continuing DAG without waiting for today's update ({today})."
                    )
                    return f"************Continuing at 9 AM without waiting for update ({today})."

                data = self.client.get_api_information_status()

                # Check all collections for dateDerniereMiseADisposition
                dates_dernieres_mises_a_jour = data.get(
                    "datesDernieresMisesAJourDesDonnees", []
                )

                if not dates_dernieres_mises_a_jour:
                    raise ValueError(
                        "No datesDernieresMisesAJourDesDonnees found in API response"
                    )

                # Check and log all dates
                logging.info(
                    f"Checking API availability for today ({today}). Found {len(dates_dernieres_mises_a_jour)} collections:"
                )
                all_today = True
                for collection_data in dates_dernieres_mises_a_jour:
                    date_str = collection_data.get("dateDerniereMiseADisposition")
                    if date_str:
                        # Parse the date (format: "2026-01-02T07:40:39.000")
                        date_obj = datetime.fromisoformat(
                            date_str.replace("Z", "+00:00")
                        ).date()
                        is_today = date_obj == today
                        status = "✓" if is_today else "✗"
                        logging.info(
                            f"  {status} dateDerniereMiseADisposition = {date_str} "
                            f"(parsed: {date_obj}, is_today: {is_today})"
                        )
                        if not is_today:
                            all_today = False
                    else:
                        logging.warning(" ⚠⚠⚠⚠⚠ No dateDerniereMiseADisposition found")
                        all_today = False

                if all_today:
                    # All collections have today's date - DAG can continue
                    return f"All collections have today's date ({today}). API is ready."

                # Wait 5 minutes before retrying
                logging.info(
                    f"Data not yet available for today ({today}). Waiting 5 minutes before retry..."
                )
                time.sleep(300)  # 5 minutes in seconds

            except ValueError as e:
                error_msg = f"Error processing API response: {str(e)}"
                logging.error(error_msg)
                raise ValueError(error_msg) from e
            except Exception as e:
                # Handle any other exceptions from API calls
                error_msg = f"Unexpected error checking API availability: {str(e)}"
                logging.error(error_msg)
                raise ValueError(error_msg) from e

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
