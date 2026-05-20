Ces jeux de données contiennent certaines informations sur les entreprises françaises disponibles dans [l'Annuaire des Entreprises](https://annuaire-entreprises.data.gouv.fr/) et dans son [API de Recherche](https://api.gouv.fr/les-api/api-recherche-entreprises).

Les données proviennent de plusieurs [sources de données](https://annuaire-entreprises.data.gouv.fr/donnees/sources), soit à la maille de l’unité légale (siren) soit à la maille de l’établissement (siret).

L’équipe data.gouv.fr publie ces jeux de données afin de simplifier la jointure entre les différents jeux de données publics utilisant le SIREN ou le SIRET.

**Attention**, il ne s'agit pas ici d'une fusion de tous ces jeux de données.

* Unité légale :

  * Base Sirene :

    * SIREN

    * SIRET du siège

    * Date de mise a jour des données dans la base sirene

    * État administratif (ACTIVE/CESSÉE)

    * Statut de diffusion (OUI/NON)

    * Nombre d’établissements

    * Entreprise individuelle (OUI/NON)

    * Association (OUI/NON et numéro RNA)

  * Champ calculé à partir des données de la base Sirene :

    * Nom complet affiché sur Annuaire des Entreprises

  * Autre sources :

    * Date de mise à jour des données dans le Registre National des Entreprises

    * Déclaration Egapro

    * Récépissé de licence d’entrepreneur de spectacle

    * ESS

    * Organisme de formation

    * Qualiopi

    * Entreprise Inclusive / SIAE

    * Pour les collectivités territoriales

      * Code commune

      * Elus

    * Liste de toutes les Conventions Collectives

* Etablissement :

  * Base Sirene :

    * SIRET

    * SIREN de l’unité légale

    * Siège social (OUI/NON)

    * État administratif (ACTIF/FERMÉ)

    * Statut de diffusion (OUI/NON)

  * Champs calculés à partir de la Base Adresse Nationale et des champs d’adresse de la base Sirene :

    * Adresse

    * Coordonnées GPS (latitude, longitude)

  * Autre sources :

    * FINESS

    * BIO

    * Convention collective

    * Reconnue Garant de l’Environnement

    * UAI

**Géolocalisation**

Les coordonnées GPS ne sont pas calculés de la même manière que les coordonnées Lambert publiées par l’Insee dans la [base Sirene](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/).

La plupart des numéros Siret utilisent le géocodage (latitude/longitude) provenant de la base SIRENE [géocodée par data.gouv](https://www.data.gouv.fr/fr/datasets/base-sirene-des-etablissements-siret-geolocalisee-avec-la-base-dadresse-nationale-ban/). Cependant, pour ceux modifiés après le début du mois en cours, la géolocalisation est directement extraite de la base SIRENE, en convertissant les coordonnées Lambert en coordonnées GPS.

**Fréquence de publication**

Ces données sont mises à jour quotidiennement. Ce jeu de données représente un extrait partiel des informations utilisées par l'Annuaire. D'autres évolutions sont prévues pour ces fichiers, avec l'intégration de nouveaux champs.
