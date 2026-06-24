import logging

from data_pipelines_annuaire.helpers import (
    DataProcessor,
    Notification,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.config import (
    BODACC_CONFIG,
    PROCEDURES_COLLECTIVES_CONFIG,
    RADIATIONS_CONFIG,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.procedures_collectives import (
    process_procedures_collectives,
)
from data_pipelines_annuaire.workflows.data_pipelines.bodacc.radiations import (
    process_radiations,
)


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
        ]

    def preprocess_radiations(self):
        logging.info("Processing BODACC radiations...")
        df = process_radiations(
            self.config.files_to_download["radiations"]["destination"],
            self.CHUNK_SIZE,
        )
        logging.info(f"Radiations: {len(df)} unique SIRENs")
        df.to_csv(
            f"{self.config.tmp_folder}/{RADIATIONS_CONFIG.file_name}.csv", index=False
        )
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"radiations BODACC : {len(df)} SIREN",
        )

    def preprocess_procedures_collectives(self):
        logging.info("Processing BODACC procédures collectives...")
        df = process_procedures_collectives(
            self.config.files_to_download["procedures_collectives"]["destination"],
            self.CHUNK_SIZE,
        )
        logging.info(f"Procédures collectives: {len(df)} unique SIRENs")
        df.to_csv(
            f"{self.config.tmp_folder}/{PROCEDURES_COLLECTIVES_CONFIG.file_name}.csv",
            index=False,
        )
        DataProcessor.push_message(
            Notification.notification_xcom_key,
            description=f"procédures collectives BODACC : {len(df)} SIREN",
        )

    def send_file_to_object_storage(self):
        for sub_processor in self._sub_processors:
            sub_processor.send_file_to_object_storage()

    def compare_files_object_storage(self):
        # Liste (et non générateur) pour comparer/uploader toutes les
        # sous-sources sans court-circuit de any().
        return any(
            [
                sub_processor.compare_files_object_storage()
                for sub_processor in self._sub_processors
            ]
        )
