# BODACC

> :warning: L'exploitation de ces données est encore en phase de test et de développement.

Ce module collecte et traite les annonces publiées au [BODACC](https://www.bodacc.fr/) (Bulletin Officiel des Annonces Civiles et Commerciales).
Seules les données suivantes sont intégrées :
- radiations au RCS
- procédures collectives en cours
- créations
- l'ensemble des annonces réduit aux couples (SIREN, greffe)

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Fréquence | Quotidienne |
| Données sources | [Annonces Commerciales](https://www.bodacc.fr/explore/dataset/annonces-commerciales/) |
| Données de sorties | Object Storage |

## Pipeline

```
1. Collecte : télécharge les CSV radiations + procédures collectives + créations + annonces
2. Pré-traitement : filtre les annulations/rectificatifs
3. Applatissage des JSON : extrait les SIREN et données métiers (date de radiation, nature du jugement, etc.)
4. Classifie les annonces de procédures collectives avec `rule.yml` pour les procédures collectives
5. Masque les transferts de sièges
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
  - On récupère la date de création depuis la colonne `acte` : `dateImmatriculation`, avec fallback sur `dateCommencementActivite`
  - Pas de déduplication par SIREN
- **Annonces** :
  - Toutes familles sauf `vente`, `collective`, `divers` et `inconnue`
  - Deux colonnes téléchargées seulement (`listepersonnes`, `dateparution`)
  - Le flux est lu en streaming et ne conserve que le dernier avis par couple (SIREN, greffe), ce qui suffit à la règle

## 4. Classification des procédures collectives

Les règles sont évaluées dans l'ordre. La première correspondance détermine le statut. Chaque règle a :
- `nature` (obligatoire) : correspondance exacte avec la nature du jugement
- `complement_contains` (optionnel) : le complément doit contenir cette sous-chaîne (insensible à la casse)
- `statut` : le statut assigné (`liquidation_judiciaire`, `redressement_judiciaire`, `sauvegarde`, ou `null`)

Si aucune règle ne correspond, un warning est loggé et le statut est `None`.

## 5. Transferts de Sièges

Environ 2 % des radiations n'amènent à aucune cessation d'activité. Lorsqu'une
entreprise transfère son siège hors du ressort de son greffe, deux annonces sont
publiées pour un seul événement :

- le nouveau greffe publie une `immatriculation` ou `modification`
- le greffe d'origine publie une `radiation`

Or rien dans l'annonce de radiation ne permet de la distinguer d'une vraie cessation.
Et ces deux annonces ne sont pas publiées dans un ordre précis et parfois avec plusieurs mois
voire rarement plusieurs années d'écarts.

### La règle

On part de la dernière radiation, on relève son greffe puis on regarde les annonces postérieures :

| annonces postérieures | conclusion |
| --- | --- |
| au moins une émanant d'un **autre greffe** | l'entreprise est immatriculée ailleurs → transfert = masquée |
| aucune, ou uniquement du **même greffe** | radiation réelle = conservée |

La condition sur le greffe est nécessaire car une entreprise cessée peut
tout à fait avoir des annonces postérieures : clôture de liquidation, dépôt de comptes
tardif, mais elles émanent logiquement du même greffe.

### Limite connue

Une radiation publiée par le nouveau greffe plutôt que celui d'origine échappe
à la règle : les annonces postérieures sont alors au même greffe.
Cas très rares plus compliqués à isoler. Malheureusement les annonces au BODACC ne sont pas toujours
cohérentes.
Exemple : 401226774.
