# Documentation

## data_processing_collectivite_territoriale

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer la liste des collectivités territoriales. |
| Fréquence | Quotidienne |
| Données sources | [Comptes consolidés des régions](https://www.data.gouv.fr/datasets/5f68c4ec9920494bf28021e3/#/resources/619ee62e-8f9e-4c62-b166-abc6f2b86201)<br />[Comptes consolidés des départements](https://www.data.gouv.fr/datasets/5f68c4edc9ed7984245b654b/#/resources/2f4f901d-e3ce-4760-b122-56a311340fc4)<br />[Liste des EPCI](https://www.collectivites-locales.gouv.fr/institutions/liste-et-composition-des-epci-fiscalite-propre)<br />[Liste des communes](https://www.data.gouv.fr/datasets/630f5173873064dd369479b4/#/resources/1e3493b3-7fc0-4005-8aa7-240bdf17e617) |
| Données de sorties | Minio |


## data_processing_collectivite_territoriale_elus

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer la liste des elus des collectivités territoriales. |
| Fréquence | Quotidienne |
| Données sources | [Conseillers régionaux<br />Conseillers départementaux<br />Conseillers municipaux<br />Conseillers communautaires](https://www.data.gouv.fr/datasets/5c34c4d1634f4173183a64f1) |
| Données de sorties | Minio |
