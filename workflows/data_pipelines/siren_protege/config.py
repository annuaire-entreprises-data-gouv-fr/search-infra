GRIST_DOCUMENT_ID_VARIABLE = "SIREN_PROTEGE_GRIST_DOCUMENT_ID"
GRIST_TABLE_ID_VARIABLE = "SIREN_PROTEGE_GRIST_TABLE_ID"
GRIST_COLUMN_NAMES = [
    "siren",
    "prenoms",
    "nom",
    "date_de_naissance",
    "legacy",
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
