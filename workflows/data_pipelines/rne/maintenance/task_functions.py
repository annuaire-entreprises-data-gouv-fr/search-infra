from helpers.minio_helpers import minio_client


def rename_old_rne_folders(**kwargs):
    minio_client.rename_folder("rne/flux/data-2023", "rne/flux/data")
