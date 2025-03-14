# Documentation

## data_processing_collectivite_territoriale

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer la liste des collectivités territoriales. |
| Fréquence | Quotidienne |
| Données sources | [Comptes consolidés des régions](https://www.data.gouv.fr/fr/datasets/5f68c4ec9920494bf28021e3)<br />[Comptes consolidés des départements](https://www.data.gouv.fr/fr/datasets/5f68c4edc9ed7984245b654b)<br />[Table de correspondance Siren et code Insee des communes](https://www.data.gouv.fr/fr/datasets/630f5173873064dd369479b4) |
| Données de sorties | Minio |

## data_processing_collectivite_territoriale_elus

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer la liste des elus des collectivités territoriales. |
| Fréquence | Quotidienne |
| Données sources | [Conseillers régionaux<br />Conseillers départementaux<br />Conseillers municipaux<br />Conseillers communautaires](https://www.data.gouv.fr/fr/datasets/5c34c4d1634f4173183a64f1) |
| Données de sorties | Minio |
