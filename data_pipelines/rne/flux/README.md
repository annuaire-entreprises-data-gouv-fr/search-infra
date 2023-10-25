# Documentation

## get_flux_rne

| Information | Valeur |
| -------- | -------- |
| Fichier source     | `DAG.py`     |
| Description | DAG Airflow permettant de récupérer le flux quotidien des données RNE depuis l'API. Le DAG récupère les données des SIREN modifiés depuis la dernière date de modification enregistrée dans les fichiers JSON stockés sur MinIO et jusqu'à la journée précédant l'exécution du DAG. Une fois récupérées, ces données sont sauvegardées sur une instance Minio. Ces fichiers sont par la suite utilisés pour générer le fichier flux dirigeants pour [l'annuaire des entreprises](https://annuaire-entreprises.data.gouv.fr). | Quotidien |
| Données sources | API RNE INPI |
| Données de sorties | Fichiers json dans Minio |
| Channel Mattermost d'information | ~startup-datagouv-dataeng |
