# Documentation

## data_processing_sirene_flux

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer le flux quotidien de la base SIRENE depuis l'API de l'INSEE. |
| Fréquence | Quotidienne sauf le 1er du mois car seules les données de la veille qui ne sont pas dans le stock nous intéressent |
| Données sources | [API SIRENE](https://api.insee.fr/api-sirene/3.11/) |
| Données de sorties | Minio |
