# Documentation

## data_processing_uai

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer différentes sources de données autour des établissements scolaires, les traite pour générer un fichier de synthèse complet. |
| Fréquence | Quotidienne |
| Données sources | [Annuaire de l'éducation du MENJ](https://www.data.gouv.fr/datasets/5889d03fa3a72974cbf0d5b1/)<br />[Principaux établissements d'enseignement supérieur du MESR](https://www.data.gouv.fr/datasets/586dae5ea3a7290df6f4be88/)<br />[Idéo-Structures d'enseignement supérieur de l'ONISEP](https://www.data.gouv.fr/datasets/5fa5e386afdaa6152360f323/) |
| Données de sorties | Minio |

### Note:

Les jeux de données ONISEP et MENJ sont régulièrement écrasés par de nouvelles ressources au lieu d'être mises à jours.
Nous devons donc récupérer l'identifiant de la dernière ressource depuis la page dataset pour obtenir la dernière version.
