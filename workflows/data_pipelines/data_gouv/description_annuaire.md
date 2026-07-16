**⚠️ Mise à jour du 13 mai 2026 :** les champs latitude, longitude, x et y ont été supprimés. Nous vous invitons à utiliser ce jeu de données pour récupérer les coordonnées géographiques : [Géolocalisation des établissements du répertoire SIRENE](https://www.data.gouv.fr/datasets/geolocalisation-des-etablissements-du-repertoire-sirene-pour-les-etudes-statistiques).

Ces jeux de données contiennent certaines informations sur les entreprises françaises disponibles dans [l'Annuaire des Entreprises](https://annuaire-entreprises.data.gouv.fr/) et dans son [API de Recherche](https://api.gouv.fr/les-api/api-recherche-entreprises).

Les données proviennent de plusieurs [sources de données](https://annuaire-entreprises.data.gouv.fr/donnees/sources), soit à la maille de l'unité légale (siren) soit à la maille de l'établissement (siret).

L'équipe data.gouv.fr publie ces jeux de données afin de simplifier la jointure entre les différents jeux de données publics utilisant le SIREN ou le SIRET.

**Attention**, il ne s'agit pas ici d'une fusion de tous ces jeux de données.

* Unité légale :

  * Base Sirene :

    * SIREN

    * SIRET du siège

    * Date de mise a jour des données dans la base sirene

    * État administratif (ACTIVE/CESSÉE)

    * Statut de diffusion (OUI/NON)

    * Nombre d'établissements

    * Nombre d'établissements ouverts

    * Nature juridique

    * Entreprise individuelle (OUI/NON)

    * Association (OUI/NON et numéro RNA)

    * Société à mission

    * ESS

  * Champ calculé à partir des données de la base Sirene :

    * Nom complet affiché sur Annuaire des Entreprises

    * Administration (OUI/NON)

  * Autre sources :

    * Date de mise à jour des données dans le Registre National des Entreprises

    * Déclaration Egapro

    * Achats responsables

    * Alim'Confiance

    * Récépissé de licence d'entrepreneur de spectacle

    * Entreprise du Patrimoine Vivant

    * Organisme de formation

    * Qualiopi

    * Entreprise Inclusive / SIAE

    * FINESS juridique

    * Aide ADEME

    * Avocat

    * Pour les collectivités territoriales

      * Code commune

      * Elus

    * Liste de toutes les Conventions Collectives

* Etablissement :

  * Base Sirene :

    * SIRET

    * SIREN de l'unité légale

    * Siège social (OUI/NON)

    * Ancien siège (OUI/NON)

    * État administratif (ACTIF/FERMÉ)

    * Statut de diffusion (OUI/NON)

    * Adresse : champ calculé à partir des champs d'adresse de la base Sirene :

  * Autre sources :

    * FINESS Géographique

    * BIO

    * Convention collective

    * Reconnue Garant de l'Environnement

    * UAI

**Fréquence de publication**

Ces données sont mises à jour quotidiennement. Ce jeu de données représente un extrait partiel des informations utilisées par l'Annuaire. D'autres évolutions sont prévues pour ces fichiers, avec l'intégration de nouveaux champs.
