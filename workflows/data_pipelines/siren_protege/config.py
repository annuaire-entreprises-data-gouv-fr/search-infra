from data_pipelines_annuaire.config import DataSourceConfig

GRIST_DOCUMENT_ID_VARIABLE = "SIREN_PROTEGE_GRIST_DOCUMENT_ID"
GRIST_TABLE_ID_VARIABLE = "SIREN_PROTEGE_GRIST_TABLE_ID"
GRIST_COLUMN_NAMES = [
    "siren",
    "prenoms",
    "nom",
    "date_de_naissance",
    "legacy",
    "ortographe_verifiee",
    "date_ajout",
    "motif",
    "date_validation_demande",
    "instructeur",
    "date_depot_demande",
    "numero_dossier",
]
# Columns identifying a protected dirigeant.
# Grist has one row per request, the published CSV one row per dirigeant.
KEY_COLUMNS = ["siren", "prenoms", "nom", "date_de_naissance"]
MOTIF_MAX_LENGTH = 10_000
ORTOGRAPHE_VERIFIEE_ANNOTATION = (
    "Le nom et prénoms sur le dossier matchent le nom et prénoms sur l'Annuaire."
)

SIREN_PROTEGE_CONFIG = DataSourceConfig(
    name="siren_protege",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/siren_protege",
    object_storage_path="siren_protege",
    file_name="siren_protege",
    is_public=False,
)
