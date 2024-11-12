from dag_datalake_sirene.workflows.data_pipelines.egapro.egapro_processor import (
    EgaproProcessor,
)

egapro_processor = EgaproProcessor()


def preprocess_egapro_data(ti):
    egapro_processor.preprocess_data(ti)


def save_date_last_modified():
    egapro_processor.save_date_last_modified()


def send_file_to_minio():
    egapro_processor.send_file_to_minio()


def compare_files_minio():
    return egapro_processor.compare_files_minio()


def send_notification(ti):
    egapro_processor.send_notification(ti)
