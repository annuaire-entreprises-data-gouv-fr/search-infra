from dag_datalake_sirene.helpers.utils import (
    drop_exact_duplicates,
    get_empty_string_if_none,
    normalize_date,
    normalize_string,
)


def unique_qualites(qualite_string):
    # Sometimes, `qualite` might contain the same string repeated with different
    # format. Example: "administrateur, Administrateur"
    # Keep only : "administrateur"
    if not qualite_string:
        return None
    # Split `qualite` string into multiple strings
    list_qualites = [s.strip() for s in qualite_string.split(",")]
    # Reverse list to keep the latest additions to the string first
    list_qualites.reverse()

    # Can't start with empty list
    list_qualites_unique = [list_qualites[0]]

    for qualite in list_qualites:
        list_qualites_unique_normalized = [
            normalize_string(unique_qualite) for unique_qualite in list_qualites_unique
        ]
        if normalize_string(qualite) not in list_qualites_unique_normalized:
            list_qualites_unique.append(qualite)
    return ", ".join(list_qualites_unique)


def drop_dirigeant_duplicates(list_dict_dirigeants, dirigeant_type):
    """
    Drop duplicates from `dirigeants' list of dictionaries.
    """

    # Reverse list to have latest inserted values in the beginning of the list
    list_dict_dirigeants.reverse()
    # List of unique dirigeants, to avoid cold start we insert the first `dirigeant`
    # from initial list
    list_dict_unique_dirigeants = [list_dict_dirigeants[0]]

    # Loop over all `dirigeants` dictionaries in initial list
    for dict_dirigeant in list_dict_dirigeants:
        i = 0
        add_dirigeant = 0
        if dirigeant_type == "pp":
            add_dirigeant, list_dict_unique_dirigeants = drop_dirigeant_pp_duplicates(
                i, list_dict_unique_dirigeants, dict_dirigeant, add_dirigeant
            )
        elif dirigeant_type == "pm":
            add_dirigeant, list_dict_unique_dirigeants = drop_dirigeant_pm_duplicates(
                i, list_dict_unique_dirigeants, dict_dirigeant, add_dirigeant
            )
        if add_dirigeant == 1:
            list_dict_unique_dirigeants.append(dict_dirigeant)
    return list_dict_unique_dirigeants


def drop_dirigeant_pm_duplicates(
    i, list_dict_unique_dirigeants, dict_dirigeant, add_dirigeant
):
    """
    For two `dirigeants` dictionaries to be matching, they have to share the same
    `siren` field.
    If that's the case, the `denomination` field is selected from the latest
    `dirigeant` dict.
    Both `qualite` fields are joined, and then only unique values (strings) are
    kept (after normalization).
    """

    def get_value(dictionary, key, default=""):
        return dictionary.get(key, default)

    # Before inserting a `dirigeant` dict in unique list, it has to be compared
    # to all other dictionaries in the list. Hence, the while loop.
    while i < len(list_dict_unique_dirigeants):

        dict_unique_dirigeant = list_dict_unique_dirigeants[i]

        if get_value(dict_dirigeant, "siren") != get_value(
            dict_unique_dirigeant, "siren"
        ):
            add_dirigeant = 1
            i += 1
            continue

        elif get_value(dict_dirigeant, "siren") == get_value(
            dict_unique_dirigeant, "siren"
        ):
            add_dirigeant = 0
            i += 1

            if (
                not dict_unique_dirigeant["denomination"]
                and dict_dirigeant["denomination"]
            ):
                dict_unique_dirigeant["denomination"] = dict_dirigeant["denomination"]

            if normalize_string(
                get_value(dict_dirigeant, "qualite")
            ) != normalize_string(get_value(dict_unique_dirigeant, "qualite")):
                dict_unique_dirigeant["qualite"] = ", ".join(
                    [
                        get_empty_string_if_none(dict_unique_dirigeant["qualite"]),
                        get_empty_string_if_none(dict_dirigeant["qualite"]),
                    ]
                )
            dict_unique_dirigeant["qualite"] = unique_qualites(
                dict_unique_dirigeant["qualite"]
            )
    return add_dirigeant, list_dict_unique_dirigeants


def drop_dirigeant_pp_duplicates(
    i, list_dict_unique_dirigeants, dict_dirigeant, add_dirigeant
):

    """
    For two `dirigeants` dictionaries to be matching, they have to share both the
    same `nom`  and `prenoms` values.
    If that's the case, the `date_naissance` field is selected from the latest
    `dirigeant` dict.
    Both `qualite` fields are merged, and then only unique values (strings) are kept (
    after normalization).
    """

    def get_value(dictionary, key, default=""):
        return dictionary.get(key, default)

    # Before inserting a `dirigeant` dict in unique list, it has to be compared
    # to all other dictionaries in the list. Hence, the while loop.
    while i < len(list_dict_unique_dirigeants):

        dict_unique_dirigeant = list_dict_unique_dirigeants[i]

        # Two `dirigeants are considered different if they have different surnames
        # or different first names (normalized),
        # in which case we add them to the list of unique `dirigeants`
        if (
            normalize_string(get_value(dict_dirigeant, "nom"))
            != normalize_string(get_value(dict_unique_dirigeant, "nom"))
        ) or (
            normalize_string(get_value(dict_dirigeant, "prenoms"))
            != normalize_string(get_value(dict_unique_dirigeant, "prenoms"))
        ):
            add_dirigeant = 1
            i += 1
            continue

        # Two `dirigeants are considered equal if they have the same surnames
        # and the same first names (normalized),
        # in which case we keep the last `date_naissance` and join the
        # `qualite` field; keeping only unique strings.
        elif (
            normalize_string(get_value(dict_dirigeant, "nom"))
            == normalize_string(get_value(dict_unique_dirigeant, "nom"))
        ) & (
            normalize_string(get_value(dict_dirigeant, "prenoms"))
            == normalize_string(get_value(dict_unique_dirigeant, "prenoms"))
        ):
            add_dirigeant = 0
            i += 1

            if (
                not dict_unique_dirigeant["date_naissance"]
                and dict_dirigeant["date_naissance"]
            ):
                dict_unique_dirigeant["date_naissance"] = dict_dirigeant[
                    "date_naissance"
                ]

            if normalize_string(
                get_value(dict_dirigeant, "qualite")
            ) != normalize_string(get_value(dict_unique_dirigeant, "qualite")):
                dict_unique_dirigeant["qualite"] = ", ".join(
                    [
                        get_empty_string_if_none(dict_unique_dirigeant["qualite"]),
                        get_empty_string_if_none(dict_dirigeant["qualite"]),
                    ]
                )
            dict_unique_dirigeant["qualite"] = unique_qualites(
                dict_unique_dirigeant["qualite"]
            )
    return add_dirigeant, list_dict_unique_dirigeants
