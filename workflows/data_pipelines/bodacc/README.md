# BODACC

> :warning: L'exploitation de ces données est encore en phase de test et de développement.

Ce module collecte et traite les **radiations**, **procédures collectives** et **créations** publiées au [BODACC](https://www.bodacc.fr/) (Bulletin Officiel des Annonces Civiles et Commerciales).
Seules les données suivantes sont intégrées :
- radiations au RCS
- procédures collectives en cours
- créations (toutes, utilisées pour fiabiliser la visibilité des radiations)

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Fréquence | Quotidienne |
| Données sources | [Annonces Commerciales](https://www.bodacc.fr/explore/dataset/annonces-commerciales/) |
| Données de sorties | Object Storage |

## Pipeline

```
1. Collecte : télécharge les CSV radiations + procédures collectives + créations
2. Pré-traitement : filtre les annulations/rectificatifs
3. Applatissage des JSON : extrait les SIREN et données métiers (date de radiation, nature du jugement, etc.)
4. Classifie les annonces de procédures collectives avec `rule.yml` pour les procédures collectives
```

## 1. Téléchargement

Depuis : https://www.bodacc.fr/explore/dataset/annonces-commerciales/information/
En appliquant des filtres en amont pour ne télécharger que les annonces de la famille souhaitée.

## 2. Pré-traitement

Le pipeline gère trois types de corrections d'annonces :

- **Annulations** (`typeavis = "annulation"`) : l'annonce précédente (référencée par `parutionavisprecedent`) est exclue, ainsi que l'avis d'annulation lui-même
- **Rectificatifs** (`typeavis = "rectificatif"`) : l'annonce précédente est exclue, mais le rectificatif est conservé car il contient la valeur à jour. Sauf pour les rectificatifs de type "rapport de radiation d'office" qui sont aussi exclus (équivalent à une annulation)
- **Rétractations sur tierce opposition** (procédures collectives uniquement) : l'annonce précédente est exclue ainsi que l'avis de rétractation

**Correction d'encodage** : réparation des chaînes UTF-8 mal décodées en Latin-1 (mojibake, ex : `clÃ´ture` → `clôture`).

## 3. Extraction

- Extraction et nettoyage des SIREN depuis le champ JSON `listepersonnes` (champ `numeroImmatriculation.numeroIdentification` ou `inscriptionRM.numeroIdentificationRM`).
- Déduplication par SIREN en conservant le jugement le plus récent
- **Radiations** :
  - Parsing de la date de cessation d'activité depuis le JSON `radiationaurcs` (disponible uniquement pour les personnes physiques)
- **Procédures Collectives** :
  - Exclusion des familles non pertinentes : `Avis de dépôt`, `Loi de 1967`, etc.
  - Détection des clôtures : les procédures dont la `famille` contient "jugement de clôture" sont marquées `is_cloture`
  - Expiration : les procédures de plus de 10 ans sont marquées `is_expired`
- **Créations** :
  - Parsing de la date de création depuis le JSON `acte` (`dateImmatriculation`, avec repli sur `dateCommencementActivite`)
  - Toutes les annonces sont conservées (pas de déduplication par SIREN)
  - Table `bodacc_creations` exploitée en post-traitement des radiations : une radiation dont le SIREN porte une création BODACC plus récente est masquée (`visibility = 0`, `ei_with_bodacc_creation_since_radiation`). La table étant interrogée par le post-traitement, `CREATIONS_CONFIG` est créée avant `RADIATIONS_CONFIG` dans l'ETL.

## 4. Classification des procédures collectives

Les règles sont évaluées dans l'ordre. La première correspondance détermine le statut. Chaque règle a :
- `nature` (obligatoire) : correspondance exacte avec la nature du jugement
- `complement_contains` (optionnel) : le complément doit contenir cette sous-chaîne (insensible à la casse)
- `statut` : le statut assigné (`liquidation_judiciaire`, `redressement_judiciaire`, `sauvegarde`, ou `null`)

Si aucune règle ne correspond, un warning est loggé et le statut est `None`.
