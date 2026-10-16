# Documentation

## data_processing_opco

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Description | Ce traitement permet de récupérer l'opérateur de compétences (OPCO) de rattachement des établissements, établi par France compétences à partir de l'IDCC déclaré en DSN. |
| Fréquence | Quotidienne (source mise à jour mensuellement) |
| Données sources | [Table SIRET-OPCO](https://www.data.gouv.fr/datasets/table-siret-opco) |
| Données de sorties | Object Storage |

### Les OPCO

Les opérateurs de compétences (OPCO) sont des organismes agréés par l'État, chargés d'accompagner la formation professionnelle et l'apprentissage. Ils financent notamment les contrats d'apprentissage et de professionnalisation, et aident les entreprises de moins de 50 salariés à définir leurs besoins en formation.

Il existe 11 OPCO, chacun couvrant un ensemble de branches professionnelles : AFDAS, AKTO, ATLAS, CONSTRUCTYS, L'OPCOMMERCE, OCAPIAT, OPCO 2I, OPCO EP, OPCO MOBILITES, OPCO SANTE, UNIFORMATION COHESION SOCIALE.

Un employeur adhère à l'OPCO de la branche dont relève sa convention collective. France compétences établit la correspondance SIRET / OPCO à partir de la déclaration sociale nominative (DSN, rubrique S21.G00.11.022), qui contient pour chaque établissement :
- soit l'identifiant de la convention collective (IDCC) applicable, rattaché à un OPCO via la table de correspondance tenue par France compétences ;
- soit une valeur d'échappement (`5100`, `5501`, `9998`, `9999`) à laquelle le déclarant associe directement un OPCO.

Le rattachement est contrôlé par France Compétences au regard de l'activité principale (APE). La table est publiée au plus tard deux mois après la DSN : la donnée a donc toujours un décalage de deux à trois mois.

Trois situations :
- **SIRET rattaché à un OPCO** : `OPCO_PROPRIETAIRE` est renseigné, `OPCO_GESTION` l'est parfois (voir ci-dessous).
- **SIRET en anomalie déclarative** : `IDCC` et les OPCO sont vides. C'est le cas d'un IDCC non rattachable à un OPCO (nombreux employeurs publics, VRP, etc.), d'une valeur d'échappement sans OPCO, d'un IDCC ou d'un APE inconnu, ou de l'absence d'IDCC. Ces lignes sont ignorées.
- **SIRET absent** : aucune DSN depuis octobre 2022, ou SIREN ayant cessé son activité une année précédente.

Bien qu'un établissement puisse avoir plusieurs conventions collectives il ne peut être lié qu'à un seul OPCO.
L'IDCC retenu pour l'OPCO ne fait pas toujours partie de ses conventions collectives. On ne peut donc pas déduire l'OPCO de la liste des conventions collectives. C'est pourquoi utiliser la table de correspondance de France Compétences est nécessaire.

### La fichier de sortie

| Champ | Source | Description |
| -------- | -------- | -------- |
| `siret` | `SIRET` | SIRET de l'établissement |
| `siren` | `SIRET` | SIREN |
| `opco` | `OPCO_PROPRIETAIRE` | OPCO de rattachement : celui qui a l'établissement dans son référentiel d'adhérents. |
| `opco_gestion` | `OPCO_GESTION` | OPCO délégataire de gestion, renseigné uniquement en Outre-mer, là où l'OPCO de rattachement n'est pas toujours présent. Il gère alors les demandes de prise en charge pour son compte. |

L'`IDCC` n'est pas conservé car la liste exhaustive des conventions collectives par établissement sont déjà récupérées avec le DAG `data_processing_convention_collective`.

Une entreprise peut avoir plusieurs OPCO si ses établissements relèvent de branches différentes, d'où `liste_opco` au niveau de l'unité légale.

### Documentation

* https://travail-emploi.gouv.fr/les-operateurs-de-competences-opco
* https://www.data.gouv.fr/datasets/table-siret-opco?resource_id=763e1d17-7168-43fb-8d68-595093ca31f7
