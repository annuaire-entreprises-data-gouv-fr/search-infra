from data_pipelines_annuaire.helpers.object_storage import ObjectStorageClient


def rename_old_rne_folders():
    ObjectStorageClient().rename_folder("rne/flux/data-2023", "rne/flux/data")
