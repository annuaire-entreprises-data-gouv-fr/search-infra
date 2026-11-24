# Dirigeants protégés : demandes Démarche Numérique

Un dirigeant personne physique peut demander que ses données personnelles soient masquées sur l'Annuaire. Il en fait la demande avec un formulaire [Démarche Numérique](https://demarche.numerique.gouv.fr/). Un instructeur accepte ou refuse chaque demande.

Ce DAG copie les demandes acceptées dans la table de référence correspondante sur Grist. Une fois une demande copiée, celle ci est archivée dans Démarche Numérique. Le DAG publie ensuite la table Grist sur l'Object Storage, sous forme d'un fichier CSV privé avec une ligne par dirigeant.

| Information | Valeur |
| -------- | -------- |
| Fichier source | `dag.py` |
| Fréquence | Toutes les heures en production. Ailleurs lancement manuel uniquement. |
| Données sources | API GraphQL de Démarche Numérique, [API Recherche d'entreprises](https://recherche-entreprises.api.gouv.fr) pour la nature juridique |
| Données de sorties | Table Grist et fichier CSV sur l'Object Storage |

## Parcours d'une demande

```
1. Le dirigeant dépose sa demande sur Démarche Numérique
2. Un instructeur l'accepte ou refuse
3. Le DAG récupère les demandes acceptées et pas encore archivées
4. Il contrôle chaque demande (champs du formulaire, nature juridique de l'entreprise)
5. Il ajoute une ligne dans Grist pour chaque demande valide
6. Il archive la demande dans Démarche Numérique
7. Il envoie sur Tchap un seul message avec le nombre de demandes insérées et la liste de celles à traiter à la main ou qui méritent une attention particulière
8. Il publie la liste des dirigeants sur l'Object Storage sous format CSV
```

Les demandes refusées, en cours d'instruction ou déjà archivées ne sont jamais lues.

## Informations reprises dans Grist

Grist contient une ligne par demande.

| Colonne | Origine |
| --- | --- |
| `siren` | Champ "Numéro SIRET" du formulaire, ramené au SIREN (9 premiers chiffres) |
| `prenoms`, `nom` | Identité du demandeur saisie à l'étape "identité" de Démarche Numérique, en majuscules et sans espaces superflus. Si la demande est déposée par un tiers, c'est l'identité de la personne concernée qui est reprise |
| `date_de_naissance` | Champ "Date de naissance" du formulaire, au format `AAAA-MM-JJ` |
| `motif` | Champ "Motif de la demande", nettoyé (voir plus bas) |
| `date_depot_demande` | Date de dépôt de la demande |
| `date_validation_demande` | Date de la décision d'acceptation |
| `instructeur` | Email de l'agent qui a pris la dernière décision |
| `numero_dossier` | Numéro unique du dossier Démarche Numérique |
| `date_ajout` | Date de l'ajout dans Grist |
| `legacy` | Toujours `false`. Les lignes `true` viennent de l'ancienne logique Siren protégés |
| `ortographe_verifiee` | Annotation privée Oui / Non par laquelle l'instructeur confirme que les nom et prénoms du dossier correspondent à ceux de l'Annuaire (RNE). `false` par défaut. |

⚠️ Le libellé de chaque champ du formulaire est sa clé d'identification, donc changer le nom d'un libellé fera échouer le DAG.

## Garanties

- **Pas d'archivage sans insert au préalable dans Grist** : si l'ajout dans Grist échoue, la demande n'est pas archivée et sera retentée à l'exécution suivante.
- **Pas de doublon** : avant d'ajouter une demande, le DAG vérifie que son numéro de dossier n'est pas déjà dans Grist. Si l'archivage a échoué après l'ajout, la demande est seulement archivée à l'exécution suivante.
- **Une demande en erreur ne bloque pas les autres** : les demandes sont traitées une par une.
- **Plusieurs demandes d'un même dirigeant** donnent plusieurs lignes dans Grist, une par dossier.

## Demandes mises de côté

Certaines demandes acceptées ne peuvent pas être ajoutées automatiquement. Elles ne sont ni ajoutées dans Grist ni archivées et sont notifiées sur Tchap à chaque exécution tant qu'elles n'ont pas été traitées à la main dans Démarche Numérique.

Les demandes mises de côté sont réparties en deux catégories :

- 🟠 **Cas attendus** : ils viennent de la demande elle-même et non d'un dysfonctionnement. Le DAG réussit et envoie un message Tchap.
- 🔴 **Erreurs :** elles signalent un problème à corriger dans le formulaire ou dans le code. Le DAG échoue et l'alerte d'échec est envoyée sur Tchap et par email.

| Situation | Catégorie | Cause | Action attendue |
| --- | --- | --- | --- |
| Siren introuvable | Cas attendu | Le SIREN ne correspond à aucune entreprise connue de l'Annuaire | Vérifier s'il s'agit d'un Siren doublon sinon voir avec le demandeur |
| Motif tronqué | Cas attendu | Le motif dépasse la limite de caractères. La demande est bien ajoutée et archivée, avec un motif coupé dans Grist. Elle n'est signalée qu'une fois | Aucune. Le texte complet reste dans le dossier Démarche Numérique |
| Nom et prénoms non confirmés | Cas attendu | L'instructeur n'a pas confirmé, en répondant "Oui" à l'annotation privée, que les nom et prénoms du dossier correspondent à ceux de l'Annuaire. La demande est bien ajoutée et archivée avec `ortographe_verifiee` à `false`. Elle n'est signalée qu'une fois | Vérifier les nom et prénoms du dossier et corriger la ligne dans Grist si besoin |
| Champ absent, vide ou invalide | Erreur | Champ obligatoire du formulaire manquant, renommé ou dans un format inattendu (Siren mal formé, date non reconnue, motif vide) | Vérifier le formulaire et le dossier, adapter le code si le formulaire a changé |
| Échec de l'ajout dans Grist ou de l'archivage | Erreur | Indisponibilité de Grist ou de Démarche Numérique | Aucune si l'erreur est passagère : la demande est retentée à l'exécution suivante. Sinon, consulter les logs |

Si l'envoi du message Tchap échoue, le DAG échoue aussi.

Les messages Tchap ne contiennent que le numéro de chaque dossier, un lien vers le dossier et la raison de la mise de côté, jamais de données personnelles.

## Suivi hebdomadaire

Le DAG `suivi_siren_protege` (`dag_suivi.py`, logique dans `suivi.py`) tourne tous les lundis à 10h heure de Paris en production. Sur les autres environnements, il ne se lance qu'à la main.
Il lit la table Grist et envoie sur le canal dTchap de support un message d'avertissement avec les dossiers dont l'ortographe des dirigeants n'a toujours pas été vérifiée (`ortographe_verifiee = false`) 4 jours ou plus après leur ajout. Après l'archivage du dossier, c'est dans Grist que l'instructeur confirme que les nom et prénoms correspondent à ceux de l'Annuaire / RNE. Un dossier sera donc signalé chaque lundi tant que la colonne n'est pas passée à `true`. Les lignes legacy ne sont pas concernées.

Il vérifie aussi que les lignes de Grist sont complètes. Une ligne n'est vérifiée que si `siren` est renseigné. Pour une ligne non legacy, toutes les colonnes sont obligatoires sauf `motif`, `prenoms` et `date_de_naissance`. Pour une ligne legacy, seuls `siren`, `date_ajout` et `legacy` le sont. Chaque ligne incomplète est signalée avec son numéro de ligne dans Grist, son siren, le lien vers son dossier s'il y en a un, et la liste des colonnes vides.

## Nettoyage du motif

Le motif est un texte libre écrit par le demandeur. Avant d'être enregistré dans Grist :

- sa mise en forme d'origine est conservée (retours à la ligne, espaces, tabulations)
- les caractères invisibles sont supprimés, pour que le texte stocké soit exactement celui qu'on lit
- les guillemets doubles `"` sont supprimés, pour que le fichier publié reste lisible par un tableur et que la conversion en CSV soit facile
- un texte commençant par `=`, `+`, `-` ou `@` est préfixé d'une apostrophe pour qu'un tableur ne l'interprète pas comme une formule
- il est tronqué à 10 000 caractères

Le motif reste un texte saisi par un tiers : il doit être échappé avant tout affichage.
