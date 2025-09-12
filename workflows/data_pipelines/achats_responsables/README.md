# Documentation

## data_processing_achats_responsables

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer les entreprises labelisées [Fournisseurs et Achats Responsables.](https://www.economie.gouv.fr/mediateur-des-entreprises/label-relations-fournisseurs-et-achats-responsables) |
| Fréquence | Quotidienne |
| Données sources | [Liste des labelisés](https://www.data.gouv.fr/datasets/liste-des-labellises-relations-fournisseurs-et-achats-responsables/) |
| Données de sorties | Minio |
| Note | Inclu uniquement les entreprises ayant un Siren associé. |
