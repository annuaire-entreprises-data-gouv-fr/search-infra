# Pipeline SIRENE/RNE – Overview

## 1. Vue d’ensemble du pipeline

SIRENE stock/flux alimente MinIO.  
RNE stock/flux alimente MinIO puis génère `rne.db`.  
L’ETL lit tout, génère `sirene.db`.  
`sirene.db` est utilisé par Elasticsearch et Data.gouv.

### Schéma ASCII

```text
[ SIRENE stock ]     [ SIRENE flux ]       [ RNE stock ]       [ RNE flux ]
      ↓                    ↓                    ↓                    ↓
  MinIO insee/stock   MinIO insee/flux    MinIO rne/stock       MinIO rne/flux
                                              ↓        ↓
                                              [ fill_rne_database ]
                                                      ↓
                                               MinIO rne/database
                                                      ↓
                           [ extract_transform_load_db (ETL Sirene) ]
                           lit SIRENE (stock+flux) + RNE (rne_<date>.db)
                                                      ↓
                                      sirene.db  →  MinIO sirene/database
                                                      ↓
                           ┌──────────────────────────┴──────────────────────┐
                           ↓                                               ↓
                 [ DAG index Elasticsearch ]                 [ DAG publish_data_gouv ]
```

---

## 2. Producteurs de données

### Jeux de données majeurs

| DAG | Chemin | Schedule | Source consommée | Sortie MinIO |
|-----|--------|----------|-------------------|---------------|
| data_processing_sirene_stock | workflows/data_pipelines/sirene/stock | 0 0 * * * | ZIP INSEE | insee/stock/ |
| data_processing_sirene_flux | workflows/data_pipelines/sirene/flux | 0 4 * * * | API INSEE flux | insee/flux/ + metadata |
| get_rne_stock | workflows/data_pipelines/rne/stock | Manuel | ZIP INPI | rne/stock/ |
| get_flux_rne | workflows/data_pipelines/rne/flux | 0 1 * * * | API RNE | rne/flux/ |
| fill_rne_database | workflows/data_pipelines/rne/database | 0 2 * * * | rne/stock + rne/flux | rne/database/ + latest_rne_date.json |

`get_rne_stock` est configuré sans planification automatique (`schedule_interval=None`).

Format cron :  
`0 4 * * *` = Tous les jours à 4h.

---

## 3. Jeux de données secondaires

Ces DAGs représentent des sources complémentaires, non critiques pour la construction du cœur du pipeline SIRENE/RNE, mais utilisées par l’ETL pour enrichir la base SQLite finale lorsque les tables correspondantes sont activées.

Ils suivent le schéma :  
- téléchargement d’un dataset externe  
- dépôt dans un dossier MinIO dédié  
- consommation optionnelle par l’ETL via `DatabaseTableConstructor`.

| Dataset | DAG | Chemin | Sortie MinIO |
|---------|------|---------|---------------|
| Bilans financiers | data_processing_bilans_financiers | workflows/data_pipelines/bilans_financiers/ | bilans_financiers/ |
| Convention collective (métadonnées) | get_metadata_cc | workflows/data_pipelines/metadata/cc/ | convention_collective/ |
| Patrimoine vivant | patrimoine_vivant | workflows/data_pipelines/patrimoine_vivant/ | patrimoine_vivant/ |
| UAI | uai | workflows/data_pipelines/uai/ | uai/ |
| Finess | finess | workflows/data_pipelines/finess/ | finess/ |
| Agence bio | agence_bio | workflows/data_pipelines/agence_bio/ | agence_bio/ |
| Alim Confiance | alim_confiance | workflows/data_pipelines/alim_confiance/ | alim_confiance/ |
| RGE | rge | workflows/data_pipelines/rge/ | rge/ |
| ESS France | ess_france | workflows/data_pipelines/ess_france/ | ess/ |
| Marché de l’inclusion | marche_inclusion | workflows/data_pipelines/marche_inclusion/ | marche_inclusion/ |
| Colter | colter | workflows/data_pipelines/colter/ | colter/ + colter_elus/ |
| Formation | formation | workflows/data_pipelines/formation/ | formation/ |

Rôle dans le pipeline :  
- optionnels  
- consommés uniquement dans l’ETL  
- enrichissent la base finale  
- ne participent pas à la consolidation RNE ou SIRENE

---

## 4. Hors périmètre de l’analyse

Les répertoires suivants existent mais ne sont **pas nécessaires** pour comprendre le pipeline SIRENE/RNE → SQLite → Elasticsearch :

```
workflows/data_pipelines/achats_responsables
workflows/data_pipelines/agence_bio
workflows/data_pipelines/alim_confiance
workflows/data_pipelines/bilan_ges
workflows/data_pipelines/bilan_financiers
workflows/data_pipelines/colter
workflows/data_pipelines/convcollective
workflows/data_pipelines/egapro
workflows/data_pipelines/ess_france
workflows/data_pipelines/finess
workflows/data_pipelines/formation
workflows/data_pipelines/marche_inclusion
workflows/data_pipelines/patrimoine_vivant
workflows/data_pipelines/rge
workflows/data_pipelines/spectacle
workflows/data_pipelines/uai
```

Ils correspondent à des enrichissements additionnels et non à la mécanique centrale.

---

## 5. Pipeline ETL Sirene (cœur du système)

### Entrées
- MinIO `insee/stock/`
- MinIO `insee/flux/`
- MinIO `rne/database/` (via `get_rne_database()`)
- autres datasets optionnels

### Processus
- charge SIRENE stock → crée tables SQLite  
- applique flux SIRENE → mises à jour  
- charge RNE → enrichit  
- ajoute sources secondaires via `DatabaseTableConstructor`  
- contrôles  
- archive `sirene.db`  
- upload MinIO `sirene/database/`

### Sortie
- `sirene.db.gz`
- déclenchement de l’indexation ES

---

## 6. Consommateurs

### 1. Elasticsearch
- déclenché par `trigger_indexing_dag`
- lit `sirene/database/sirene.db.gz`
- reconstruit / met à jour l’index

### 2. Data.gouv
- DAG `publish_files_in_data_gouv`
- lit la dernière version de `sirene.db`
- génère des CSV et publie sur data.gouv

