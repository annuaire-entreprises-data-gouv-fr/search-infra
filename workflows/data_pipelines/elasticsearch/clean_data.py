import logging

from dag_datalake_sirene.helpers.utils import normalize_string


def unique_qualites(qualite_string):
    # Sometimes, `qualite` might contain the same string repeated with different
    # format. Example: "administrateur, Administrateur"
    # Keep only : "administrateur"
    if not qualite_string:
        return None
    # Split `qualite` string into multiple strings
    list_qualites = [s.strip() for s in qualite_string.split(",")]

    # Create dictionary with normalized qulity string as key and all corresponding
    # qualities as values (list of non-normalized qualities)

    qualites = {}

    for qualite in list_qualites:
        if normalize_string(qualite) in qualites:
            qualites[normalize_string(qualite)].append(qualite)
        else:
            qualites[normalize_string(qualite)] = [qualite]

    return ", ".join([qualites[qualite][-1] for qualite in qualites])


def drop_duplicates_personnes_physiques(list_dict_personnes):
    """
    We want to identify partial duplicates:
    For two `personnes_physiques` dictionaries to be matching, they have to share
    both the same `nom` and `prenoms` values.
    If that's the case, the `date_naissance` field is selected from the latest
    `personne_physique` dict.
    Both `role_description` fields are merged, and then only unique values (strings)
    are kept after normalization.

    We use a dictionary to detect partial duplicates;
    Each key contains a normalized "nom+prenoms" string and the corresponding
    values are all the "personnes_physiques" dictionaries with the same "nom"
    and "prenoms" un-normalized.

    We normalize the "nom+prenoms" for each personne_physique in the
    "list_dict_personnes" list, and we compare it to the existing keys in the
    `personnes_by_nom_prenom`
    dictionary.
    If it exists, we add it to the list of values for the key in question. Otherwise, we
    create another key with the normalized "nom+prenoms" string using the
    dictionary's values.

    We then merge the values to create one "personne_physique" per key, by using the
    data from the last inserted "personne_physique" and joining the rest of the fields.
    """
    personnes_by_nom_prenom = {}
    for personne in list_dict_personnes:
        normalized_name = (
            f"{normalize_string(personne['nom'])}_"
            f"{normalize_string(personne['prenoms'])}"
        )
        if normalized_name in personnes_by_nom_prenom:
            personnes_by_nom_prenom[normalized_name].append(personne)
        else:
            personnes_by_nom_prenom[normalized_name] = [personne]

    list_personnes_unique = []
    for key_personne, same_personnes in personnes_by_nom_prenom.items():
        last_personne = same_personnes[-1]
        if len(same_personnes) == 1:
            list_personnes_unique.append(last_personne)
        else:
            unique_personne = {
                "nom": last_personne["nom"],
                "prenoms": last_personne["prenoms"],
            }

            list_qualites = [
                personne.get("role_description", "") for personne in same_personnes
            ]
            # using list comprehension to remove None values in list
            list_qualites_filtered = [
                qualite for qualite in list_qualites if qualite is not None
            ]

            unique_personne["role_description"] = unique_qualites(
                ", ".join(list_qualites_filtered)
            )

            dates = list(
                {
                    personne["date_de_naissance"]
                    for personne in same_personnes
                    if personne["date_de_naissance"]
                }
            )
            if len(dates) > 1:
                logging.debug(
                    f"At least two personnes with same name, firstname but different "
                    f"date: {dates}, siren {unique_personne}"
                )

            unique_personne["date_de_naissance"] = dates[-1] if len(dates) > 0 else None
            list_personnes_unique.append(unique_personne)
    return list_personnes_unique


def drop_duplicates_dirigeants_pm(list_dict_dirigeants):
    """
    For two "dirigeants" dictionaries to be matching, they have to share the same
    "siren" field.
    If that's the case, the `denomination` field is selected from the latest
    `dirigeant` dict.
    Both `qualite` fields are joined, and then only unique values (strings) are
    kept (after normalization).

    We use a dictionary to detect partial duplicates;
    Each key contains a a "siren" string and the corresponding
    values are all the "dirigeants" dictionaries with the same "siren" value.

    We compare each "siren" (dirigeant) in "list_dict_dirigeants" with existing keys in
    the `dirigeants_by_siren" dictionary.
    If it exists we add the entire dictionary to the list of values for the key("siren")
    in question.
    Otherwise, we create another key with the new "siren" string, and add the
    dirigeant dict to its list of values.

    We then merge the values to create one "dirigeant" per key, by using the data
    from the last inserted "dirigeant" and joining the rest of the fields.

    """
    dirigeants_by_siren = {}
    for dirigeant in list_dict_dirigeants:
        siren = f"{dirigeant['siren']}"
        if siren in dirigeants_by_siren:
            dirigeants_by_siren[siren].append(dirigeant)
        else:
            dirigeants_by_siren[siren] = [dirigeant]

    list_dirigeants_unique = []
    for key_dirigeant, same_dirigeants in dirigeants_by_siren.items():
        last_dirigeant = same_dirigeants[-1]
        if len(same_dirigeants) == 1:
            list_dirigeants_unique.append(last_dirigeant)
        else:
            unique_dirigeant = {
                "siren": last_dirigeant["siren"],
                "denomination": last_dirigeant["denomination"],
            }
            list_qualites = [
                dirigeant.get("role_description", "") for dirigeant in same_dirigeants
            ]
            # using list comprehension to remove None values in list
            list_qualites_filtered = [
                qualite for qualite in list_qualites if qualite is not None
            ]

            unique_dirigeant["role_description"] = unique_qualites(
                ", ".join(list_qualites_filtered)
            )

            list_dirigeants_unique.append(unique_dirigeant)
    return list_dirigeants_unique
