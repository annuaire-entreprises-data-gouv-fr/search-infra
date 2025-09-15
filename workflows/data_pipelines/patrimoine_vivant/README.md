# Documentation

## data_processing_patrimoine_vivant

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer les entreprises labelisées [Entreprise du Patrimoine Vivant.](https://www.entreprises.gouv.fr/espace-entreprises/s-informer-sur-la-reglementation/le-label-entreprise-du-patrimoine-vivant) |
| Fréquence | Quotidienne |
| Données sources | [Liste des labelisés](https://www.data.gouv.fr/datasets/entreprises-du-patrimoine-vivant-epv/) |
| Données de sorties | Minio |
| Note | Inclu uniquement les entreprises ayant un Siren associé. |
