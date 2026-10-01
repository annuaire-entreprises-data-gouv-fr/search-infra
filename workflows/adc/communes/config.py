from airflow.sdk import Variable

from data_pipelines_annuaire.config import (
    AIRFLOW_DAG_FOLDER,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
)

ADC_COMMUNES_DAG_NAME = "adc_communes"
ADC_COMMUNES_DATA_DIR = (
    AIRFLOW_DAG_TMP + AIRFLOW_DAG_FOLDER + ADC_COMMUNES_DAG_NAME + "/data/"
)

# Clé complète dans le bucket : hors du préfixe ae/ de l'Annuaire des Entreprises
ADC_COMMUNES_OBJECT_STORAGE_PATH = f"ac/{AIRFLOW_ENV}/communes/"

SIRENE_DATABASE_LOCATION = f"{ADC_COMMUNES_DATA_DIR}sirene.db"
WORK_DATABASE_LOCATION = f"{ADC_COMMUNES_DATA_DIR}adc_communes.db"
JSON_OUTPUT_DIR = f"{ADC_COMMUNES_DATA_DIR}communes/"

# Code officiel géographique : une ressource par millésime, on prend la plus récente
COG_DATASET_ID = "58c984b088ee386cdb1261f3"
COG_RESOURCE_TITLE = "Liste des communes, arrondissements municipaux, communes déléguées et communes associées"
COG_FILE = f"{ADC_COMMUNES_DATA_DIR}cog_communes.csv"
# Collectivités d'outre-mer (Saint-Pierre-et-Miquelon, Saint-Barthélemy, Saint-Martin,
# Polynésie, Nouvelle-Calédonie, Wallis-et-Futuna), absentes de la liste principale
COG_COMER_RESOURCE_TITLE = (
    "Liste des communes des collectivités et territoires français d'outre-mer"
)
# Communes et circonscriptions de Wallis-et-Futuna ; les districts des TAAF, Clipperton
# et l'Île des Faisans n'ont pas d'établissements
COG_COMER_NATURES = ["COM", "CIR"]
COG_COMER_FILE = f"{ADC_COMMUNES_DATA_DIR}cog_communes_comer.csv"

# https://www.data.gouv.fr/datasets/nombre-detablissements-employeurs-et-effectifs-salaries-du-secteur-prive-par-commune-x-ape-au-31-12-depuis-2006
EFFECTIFS_RESOURCE_ID = "2757f3fd-cbd3-479c-9824-893639d3c456"
EFFECTIFS_FILE = f"{ADC_COMMUNES_DATA_DIR}effectifs_salaries.csv"

FLUX_YEARS = 5

# Une commune = un dossier {code}/ contenant un JSON par type de donnée, toujours
# présent même sans donnée. Valeur : source affichée par le front.
SOURCE_SIRENE = "Insee, base Sirene des entreprises et de leurs établissements"
SOURCE_EFFECTIFS = (
    "Urssaf, établissements employeurs et effectifs salariés du secteur privé"
)
DONNEES_SOURCES = {
    "effectif_salaries": SOURCE_EFFECTIFS,
    "etablissements_sirene": SOURCE_SIRENE,
    "flux_ouverture_etablissements": SOURCE_SIRENE,
}

# Pool de connexions par défaut du client boto3 : au-delà, les connexions sont jetées
UPLOAD_THREADS = 10
# Au-dessus de ce seuil boto3 envoie en plusieurs parties et l'ETag n'est plus le MD5
# du fichier, ce qui empêcherait de détecter les fichiers inchangés (Paris ≈ 18 Mo)
UPLOAD_MULTIPART_THRESHOLD = 256 * 1024**2
# Garde-fou avant de publier et de supprimer les fichiers obsolètes (~34 900 communes)
MIN_EXPECTED_COMMUNES = 34_000
# Réglable pour tester sur un échantillon de sirene.db
MIN_EXPECTED_ETABLISSEMENTS = int(
    Variable.get("ADC_MIN_EXPECTED_ETABLISSEMENTS", 1_000_000)
)
