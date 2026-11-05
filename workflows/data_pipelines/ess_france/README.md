# Documentation

## data_processing_ess_france

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement récupère la liste des entreprises de l'Économie Sociale et Solidaire (ESS) publiée par ESS France sous forme de plusieurs classeurs Excel régionaux. |
| Fréquence | Quotidienne |
| Données sources | [Liste des entreprises de l'ESS](https://www.ess-france.org/fr/la-liste-des-entreprises-de-less) |
| Données de sorties | Object Storage |

## Traitement

- Tous les liens `.xlsx` de la page source sont téléchargés. Un par région, les noms des fichiers ne sont pas homogènes.
- Seule la colonne `N°SIREN` de la première feuille de chaque classeur est lue. Elle est dédoublonnée et triée.
- La date de dernière modification est la plus récente des en-têtes `Last-Modified` des classeurs.
- Le traitement échoue si le nombre de SIREN baisse de plus de 5 % par rapport au fichier précédent (classeur manquant ou tronqué).
- Le fichier final sur l'object storage ne change que si la liste change.
