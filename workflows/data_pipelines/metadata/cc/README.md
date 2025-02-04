# Documentation

## get_metadata_cc

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer les metadonnées sur les conventions collectives. |
| Fréquence | Tous les 3 jours entre le 2 et le 31 du mois. Cependant la mise à jour des metadonnées est désactivées dans le cas où un fichier a déjà été téléversé sur le mois. |
| Données sources | [Liste des conventions collectives et de leur code](https://travail-emploi.gouv.fr/conventions-collectives-nomenclatures) |
| Données de sorties | Minio |
