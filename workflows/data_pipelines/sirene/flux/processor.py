from datetime import datetime
import logging
import os
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.api import (
    SireneApiClient,
)
from dag_datalake_sirene.helpers.data_processor import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.config import (
    FLUX_SIRENE_CONFIG,
)
from dag_datalake_sirene.helpers.utils import (
    get_dates_since_start_of_month,
    zip_file,
)
from dag_datalake_sirene.helpers.minio_helpers import File


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
        self.current_dates = get_dates_since_start_of_month()

    @staticmethod
    def _construct_endpoint(base_endpoint: str, date: str, fields: str) -> str:
        return base_endpoint.format(date, fields)

    def get_current_flux_unite_legale(self):
        fields = (
            "siren,dateCreationUniteLegale,sigleUniteLegale,"
            "prenomUsuelUniteLegale,identifiantAssociationUniteLegale,"
            "trancheEffectifsUniteLegale,dateDernierTraitementUniteLegale,"
            "categorieEntreprise,etatAdministratifUniteLegale,nomUniteLegale,"
            "nomUsageUniteLegale,denominationUniteLegale,denominationUsuelle1UniteLegale,"
            "denominationUsuelle2UniteLegale,denominationUsuelle3UniteLegale,"
            "categorieJuridiqueUniteLegale,activitePrincipaleUniteLegale,"
            "economieSocialeSolidaireUniteLegale,statutDiffusionUniteLegale,"
            "societeMissionUniteLegale,anneeCategorieEntreprise,anneeEffectifsUniteLegale,"
            "caractereEmployeurUniteLegale,nicSiegeUniteLegale"
        )
        output_path = (
            f"{self.config.tmp_folder}flux_unite_legale_{self.current_month}.csv"
        )
        n_siren_processed = 0

        for date in self.current_dates:
            logging.info(f"{date} -- processing..")
            endpoint = self._construct_endpoint(
                self.BASE_UNITE_LEGALE_ENDPOINT, date, fields
            )
            df = self.client.fetch_data(endpoint, "unitesLegales")

            # Create a header for the first dump, append afterward
            file_exists = os.path.isfile(output_path)
            df.to_csv(output_path, mode="a", header=(not file_exists), index=False)

            n_siren = df["siren"].nunique()
            logging.info(f"{date} -- processed: {n_siren} siren")
            n_siren_processed += n_siren
            del df

        logging.info(f"CSV output ready with {n_siren_processed} siren")
        zip_file(output_path)
        logging.info("File zipped. Done!")

        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"{n_siren_processed} siren",
        )

    def get_current_flux_etablissement(self):
        fields = (
            "siren,siret,dateCreationEtablissement,trancheEffectifsEtablissement,"
            "activitePrincipaleRegistreMetiersEtablissement,etablissementSiege,"
            "anneeEffectifsEtablissement,libelleVoieEtablissement,codePostalEtablissement,"
            "numeroVoieEtablissement,dateDernierTraitementEtablissement,"
            "libelleCommuneEtablissement,libelleCedexEtablissement,typeVoieEtablissement,"
            "codeCommuneEtablissement,codeCedexEtablissement,"
            "complementAdresseEtablissement,distributionSpecialeEtablissement,"
            "complementAdresse2Etablissement,indiceRepetition2Etablissement,"
            "libelleCedex2Etablissement,codeCedex2Etablissement,"
            "numeroVoie2Etablissement,typeVoie2Etablissement,libelleVoie2Etablissement,"
            "codeCommune2Etablissement,libelleCommune2Etablissement,"
            "distributionSpeciale2Etablissement,dateDebut,etatAdministratifEtablissement,"
            "enseigne1Etablissement,enseigne1Etablissement,enseigne2Etablissement,"
            "enseigne3Etablissement,denominationUsuelleEtablissement,"
            "activitePrincipaleEtablissement,indiceRepetitionEtablissement,"
            "libelleCommuneEtrangerEtablissement,codePaysEtrangerEtablissement,"
            "libellePaysEtrangerEtablissement,libelleCommuneEtranger2Etablissement,"
            "codePaysEtranger2Etablissement,libellePaysEtranger2Etablissement,"
            "statutDiffusionEtablissement,caractereEmployeurEtablissement,"
            "coordonneeLambertAbscisseEtablissement,"
            "coordonneeLambertOrdonneeEtablissement"
        )
        output_path = (
            f"{self.config.tmp_folder}flux_etablissement_{self.current_month}.csv"
        )
        n_siret_processed = 0

        for date in self.current_dates:
            logging.info(f"{date} -- processing..")
            endpoint = self._construct_endpoint(
                self.BASE_ETABLISSEMENT_ENDPOINT, date, fields
            )
            df = self.client.fetch_data(endpoint, "etablissements")
            for prefix in ["adresseEtablissement_", "adresse2Etablissement_"]:
                df.columns = [
                    col.replace(prefix, "") if prefix in col else col
                    for col in df.columns
                ]
            # Create a header for the first dump, append afterward
            file_exists = os.path.isfile(output_path)
            df.to_csv(output_path, mode="a", header=(not file_exists), index=False)

            n_siret = df["siret"].nunique()
            logging.info(f"{date} -- processed: {n_siret} siret")
            n_siret_processed += n_siret
            del df

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
