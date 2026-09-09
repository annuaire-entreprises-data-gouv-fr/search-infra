import logging

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
    force_rebuild_requested,
)
from data_pipelines_annuaire.helpers.object_storage import ObjectStorageFile
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.annonces_couples import (
    process_annonces_couples,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.config import (
    ANNONCES_COUPLES_CONFIG,
    BODACC_CONFIG,
    CREATIONS_CONFIG,
    PROCEDURES_COLLECTIVES_CONFIG,
    RADIATIONS_CONFIG,
    build_annonces_couples_url,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.creations import (
    process_creations,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.procedures_collectives import (
    process_procedures_collectives,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.radiations import (
    process_radiations,
)

logger = logging.getLogger(__name__)


class BodaccProcessor(DataProcessor):
    CHUNK_SIZE = 100_000

    def __init__(self):
        super().__init__(BODACC_CONFIG)
        # Radiations et procédures collectives partagent le même tmp_folder mais
        # produisent chacune leur propre CSV : on délègue l'envoi et la
        # comparaison à un DataProcessor par sous-source.
        self._sub_processors = [
            DataProcessor(RADIATIONS_CONFIG),
            DataProcessor(PROCEDURES_COLLECTIVES_CONFIG),
            DataProcessor(CREATIONS_CONFIG),
            DataProcessor(ANNONCES_COUPLES_CONFIG),
        ]

    def download_data(self) -> None:
        # Un rebuild regénère l'URL pour enlever le filtre mensuel et télécharger
        # tout l'historique
        if self._rebuild_from_scratch():
            self.config.files_to_download["annonces_couples"]["url"] = (
                build_annonces_couples_url(months_window=None)
            )
            DataProcessor.push_message(
                Notification.notification_xcom_key,
                description="⚠️ Le fichier des couples greffe/siren a été reconstruit de zéro.",
            )
        super().download_data()

    @staticmethod
    def _rebuild_from_scratch() -> bool:
        """
        Le fichier des couples est reconstruit en entier lorsque le paramètre force_rebuild
        est activé ou lorsqu'il n'existe encore aucun fichier de couple sur l'object storage.
        """
        if force_rebuild_requested("annonces couples"):
            return True

        object_storage_path = (
            f"{ANNONCES_COUPLES_CONFIG.object_storage_path}/latest/"
            f"{ANNONCES_COUPLES_CONFIG.file_name}.csv"
        )
        if not ObjectStorageFile.does_exist(object_storage_path):
            logger.info(
                f"{object_storage_path} est absent de l'object storage : "
                "l'historique complet des annonces est utilisé."
            )
            return True

        return False

    @staticmethod
    def _previous_couples_url() -> str:
        """
        URL du fichier aggrégé des couples sur l'object storage
        """
        url = ANNONCES_COUPLES_CONFIG.url_object_storage
        if not url:
            raise ValueError("No object storage URL provided in the configuration.")
        return url

    def preprocess_radiations(self):
        df = process_radiations(
            self.config.files_to_download["radiations"]["destination"],
            self.CHUNK_SIZE,
        )
        df.to_csv(
            f"{self.config.tmp_folder}/{RADIATIONS_CONFIG.file_name}.csv", index=False
        )
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"radiations BODACC : {len(df)} SIREN",
        )

    def preprocess_procedures_collectives(self):
        df = process_procedures_collectives(
            self.config.files_to_download["procedures_collectives"]["destination"],
            self.CHUNK_SIZE,
        )
        df.to_csv(
            f"{self.config.tmp_folder}/{PROCEDURES_COLLECTIVES_CONFIG.file_name}.csv",
            index=False,
        )
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"procédures collectives BODACC : {len(df)} SIREN",
        )

    def preprocess_creations(self):
        df = process_creations(
            self.config.files_to_download["creations"]["destination"],
            self.CHUNK_SIZE,
        )
        df.to_csv(
            f"{self.config.tmp_folder}/{CREATIONS_CONFIG.file_name}.csv", index=False
        )
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"créations BODACC : {len(df)} annonces",
        )

    def preprocess_annonces(self):
        df = process_annonces_couples(
            self.config.files_to_download["annonces_couples"]["destination"],
            self.CHUNK_SIZE * 10,  # Nombreuses mais petites lignes
            previous_couples_url=(
                None if self._rebuild_from_scratch() else self._previous_couples_url()
            ),
        )
        df.to_csv(
            f"{self.config.tmp_folder}/{ANNONCES_COUPLES_CONFIG.file_name}.csv",
            index=False,
        )
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"annonces BODACC : {len(df)} couples (siren, greffe)",
        )

    def send_file_to_object_storage(self):
        for sub_processor in self._sub_processors:
            sub_processor.send_file_to_object_storage()

    def compare_files_object_storage(self):
        # Évaluer toutes les sous-sources avant le any() : la comparaison
        # déclenche l'upload, un court-circuit laisserait des fichiers obsolètes.
        results = [
            sub_processor.compare_files_object_storage()
            for sub_processor in self._sub_processors
        ]
        return any(results)
