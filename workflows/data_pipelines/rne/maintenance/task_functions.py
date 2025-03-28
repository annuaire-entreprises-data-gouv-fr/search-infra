from dag_datalake_sirene.helpers.minio_helpers import MinIOClient


def rename_old_rne_folders(**kwargs):
    MinIOClient().rename_folder("rne/flux/data-2023", "rne/flux/data")
