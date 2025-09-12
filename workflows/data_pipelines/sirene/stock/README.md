# Documentation

## data_processing_sirene_stock

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer les bases SIRENE de l'INSEE. |
| Fréquence | Quotidienne |
| Données sources | [Base SIRENE StockUniteLegale<br />Base SIRENE StockUniteLegaleHistorique<br />Base SIRENE StockEtablissementHistorique](https://www.data.gouv.fr/datasets/5b7ffc618b4c4169d30727e0) |
| Données de sorties | Minio |

### Note:

Le fichier StockEtablissement est récupéré indirectement dans le dag `extract_transform_load_db` via le retraitement fait par data.gouv.fr sur la [base SIRENE géolocalisée avec la Base d'Adresse Nationale (BAN)] (https://www.data.gouv.fr/datasets/base-sirene-des-etablissements-siret-geolocalisee-avec-la-base-dadresse-nationale-ban/)
