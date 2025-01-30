from datetime import datetime
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.api import (
    SireneApiClient,
)
from dag_datalake_sirene.helpers.data_processor import DataProcessor, Notification
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.config import (
    FLUX_SIRENE_CONFIG,
)
from dag_datalake_sirene.helpers.utils import (
    save_data_to_zipped_csv,
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
        self.client = SireneApiClient(self.config.url_api, self.config.auth_api)
        self.current_month = datetime.today().strftime("%Y-%m")

    def _construct_endpoint(self, base_endpoint: str, fields: str) -> str:
        return base_endpoint.format(self.current_month, fields)

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

        endpoint = self._construct_endpoint(self.BASE_UNITE_LEGALE_ENDPOINT, fields)
        df = self.client.fetch_data(endpoint, "unitesLegales")
        save_data_to_zipped_csv(
            df, self.config.tmp_folder, f"flux_unite_legale_{self.current_month}.csv"
        )
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            column=df["siren"],
            description="unités légales",
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

        endpoint = self._construct_endpoint(self.BASE_ETABLISSEMENT_ENDPOINT, fields)

        df = self.client.fetch_data(endpoint, "etablissements")
        for prefix in ["adresseEtablissement_", "adresse2Etablissement_"]:
            df.columns = [
                col.replace(prefix, "") if prefix in col else col for col in df.columns
            ]
        save_data_to_zipped_csv(
            df, self.config.tmp_folder, f"flux_etablissement_{self.current_month}.csv"
        )
        self.push_message(
            df["siret"], Notification.notification_xcom_key, "établissements"
        )

    def send_flux_to_minio(self):
        self.minio_client.send_files(
            list_files=[
                File(
                    source_path=self.config.tmp_folder,
                    source_name=f"flux_unite_legale_{self.current_month}.csv.gz",
                    dest_path=self.config.minio_path,
                    dest_name=f"flux_unite_legale_{self.current_month}.csv.gz",
                ),
                File(
                    source_path=self.config.tmp_folder,
                    source_name=f"flux_etablissement_{self.current_month}.csv.gz",
                    dest_path=self.config.minio_path,
                    dest_name=f"flux_etablissement_{self.current_month}.csv.gz",
                ),
                File(
                    source_path=self.config.tmp_folder,
                    source_name="metadata.json",
                    dest_path=self.config.minio_path,
                    dest_name="metadata.json",
                ),
            ],
        )
