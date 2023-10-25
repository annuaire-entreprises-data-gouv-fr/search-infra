# Documentation

## get_stock_rne

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG.py`     |
| Description | DAG Airflow permettant de récupérer les données stock du RNE (INPI). Le DAG récupère le fichier zip depuis le site de l'INPI, parcours les fichiers json, et les sauvegarde sur une instance Minio. Ces fichiers sont par la suite utilisés pour générer le fichier stock dirigeants pour [l'annuaire des entreprises](https://annuaire-entreprises.data.gouv.fr). | Annuel |
| Données sources | STOCK RNE INPI |
| Données de sorties | Fichiers json dans Minio |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |
