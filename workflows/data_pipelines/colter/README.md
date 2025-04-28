# Documentation

## data_processing_collectivite_territoriale

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer la liste des collectivités territoriales. |
| Fréquence | Quotidienne |
| Données sources | [Comptes consolidés des régions](https://www.data.gouv.fr/fr/datasets/5f68c4ec9920494bf28021e3)<br />[Comptes consolidés des départements](https://www.data.gouv.fr/fr/datasets/5f68c4edc9ed7984245b654b)<br />[Liste des EPCI, de leurs communes et des iles monocommunales](https://www.collectivites-locales.gouv.fr/institutions/liste-et-composition-des-epci-fiscalite-propre) |
| Données de sorties | Minio |


## data_processing_collectivite_territoriale_elus

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer la liste des elus des collectivités territoriales. |
| Fréquence | Quotidienne |
| Données sources | [Conseillers régionaux<br />Conseillers départementaux<br />Conseillers municipaux<br />Conseillers communautaires](https://www.data.gouv.fr/fr/datasets/5c34c4d1634f4173183a64f1) |
| Données de sorties | Minio |
