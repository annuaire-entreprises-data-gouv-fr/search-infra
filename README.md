# Annuaire des Entreprises - [Infrastructure de recherche]

Ce site est disponible en ligne : [L’Annuaire des Entreprises](https://annuaire-entreprises.data.gouv.fr)

Ce repository décrit le workflow qui récupère, traite et indexe les données publiques d'entreprises.

Ce code s'exécute dans une infrastructure Airflow basée sur cette stack 👉 https://github.com/etalab/data-engineering-stack.

## Architecture du service 🏗

Ce repository fait partie d'un ensemble de services qui constituent l'[Annuaire des Entreprises](https://annuaire-entreprises.data.gouv.fr) :

| Description | Accès |
|-|-|
|Le site Web | [par ici 👉](https://github.com/etalab/annuaire-entreprises-site) |
|L’API du Moteur de recherche | [par ici 👉](https://github.com/etalab/annuaire-entreprises-search-api) |
|L‘API de redondance de Sirene | [par ici 👉](https://github.com/etalab/annuaire-entreprises-sirene-api) |
|Le traitement permettant la génération de données à ingérer dans le moteur de recherche | [par ici 👉](https://github.com/etalab/annuaire-entreprises-search-infra) |

## Sources de données

L'API de recherche utilise les données de différentes administrations. Toutes les données saud les dirigeants données sont accessibles sur [data.gouv.fr](https://data.gouv.fr/).

| Données                                                          | A dministration responsable                                                                                                                                                                               |Accès au jeu de données                        |
|------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------|
| Dénomination, Adresse, NAF, Forme juridique, ESS...              | `Insee`                                                                                                                                                                                                   |https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/            |
| Dirigeants                                                       | `INPI`                                                                                                                                                                                                    |[FTP INPI](https://data.inpi.fr/content/editorial/Serveur_ftp_entreprises)|
| Liste des professionnels engagés en BIO                          | `Agence Bio`                                                                                                                                                                                              |https://www.data.gouv.fr/fr/datasets/professionnels-engages-en-bio/|
| Liste des Entreprises RGE                                        | `ADEME`                                                                                                                                                                                                   |https://www.data.gouv.fr/fr/datasets/liste-des-entreprises-rge-1/|
| Liste des entrepreneurs de spectacles vivants                    | `Ministère de la Culture`                                                                                                                                                                                 |https://www.data.gouv.fr/fr/datasets/declarations-des-entrepreneurs-de-spectacles-vivants/|
| Liste des déclarations Egapro                                    | `MTPEI`                                                                                                                                                                                                   |https://www.data.gouv.fr/fr/datasets/index-egalite-professionnelle-f-h-des-entreprises-de-50-salaries-ou-plus/|
| Liste conventions collectives                                    | `Ministère du travail`                                                                                                                                                                                    |https://www.data.gouv.fr/fr/datasets/liste-des-conventions-collectives-par-entreprise-siret/|
| Liste des organismes de formation                                | `Ministère du travail`                                                                                                                                                                                    |https://www.data.gouv.fr/fr/datasets/liste-publique-des-organismes-de-formation-l-6351-7-1-du-code-du-travail/|
| Liste des établissements du domaine sanitaire et social (FINESS) | `Ministère des Solidarités et de la Santé`                                                                                                                                                                |https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/|
| Liste des organismes de formation                                | - `Ministère de l'Éducation Nationale et de la Jeunesse` <br />-`Ministère de l'Enseignement supérieur et de la Recherche`<br />-`Office national d'information sur les enseignements et les professions` |[Annuaire de l'éducation du MENJ](https://www.data.gouv.fr/fr/datasets/5889d03fa3a72974cbf0d5b1/)<br />[Principaux établissements d'enseignement supérieur du MESR](https://www.data.gouv.fr/fr/datasets/586dae5ea3a7290df6f4be88/)<br />[Idéo-Structures d'enseignement supérieur de l'ONISEP](https://www.data.gouv.fr/fr/datasets/5fa5e386afdaa6152360f323/)|
| Liste des élus d'une collectivité territoriale                   | `Ministère de l'Intérieur et des Outre-Mer`                                                                                                                                                               |https://www.data.gouv.fr/fr/datasets/repertoire-national-des-elus-1/|
