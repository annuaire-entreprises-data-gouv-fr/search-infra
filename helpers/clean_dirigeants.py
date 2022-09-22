from dag_datalake_sirene.helpers.utils import (
    get_empty_string_if_none,
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

    # Create dictionary with normalized qulity string as key and all corresponding
    # qualities as values (list of non-normalized qualities)

    qualites = {}

    for qualite in list_qualites:
        if normalize_string(qualite) in qualites:
            qualites[normalize_string(qualite)].append(qualite)
        else:
            qualites[normalize_string(qualite)] = [qualite]

    return ", ".join([qualites[qualite][-1] for qualite in qualites])


def drop_duplicates_dirigeants_pp(list_dict_dirigeants):
    dirigeants_by_nom_prenom = {}
    for dirigeant in list_dict_dirigeants:
        normalized_name = (
            f"{normalize_string(dirigeant['nom'])}_"
            f"{normalize_string(dirigeant['prenoms'])}"
        )
        if normalized_name in dirigeants_by_nom_prenom:
            dirigeants_by_nom_prenom[normalized_name].append(dirigeant)
        else:
            dirigeants_by_nom_prenom[normalized_name] = [dirigeant]

    list_dirigeants_unique = []
    for key_dirigeant, same_dirigeants in dirigeants_by_nom_prenom.items():
        first_dirigeant = same_dirigeants[0]
        if len(same_dirigeants) == 1:
            list_dirigeants_unique.append(first_dirigeant)
        else:
            unique_dirigeant = {
                "nom": first_dirigeant["nom"],
                "prenoms": first_dirigeant["prenoms"],
                "qualite": unique_qualites(
                    ", ".join(
                        [dirigeant.get("qualite", "") for dirigeant in same_dirigeants]
                    )
                ),
            }
            dates = list(
                {
                    dirigeant["date_naissance"]
                    for dirigeant in same_dirigeants
                    if dirigeant["date_naissance"]
                }
            )
            if len(dates) > 1:
                raise Exception(
                    "At least two dirigeant with same name, firstname but different date"
                )

            unique_dirigeant["date_naissance"] = dates[0] if len(dates) > 0 else None
            list_dirigeants_unique.append(unique_dirigeant)
    return list_dirigeants_unique


def drop_duplicates_dirigeants_pm(list_dict_dirigeants):
    dirigeants_by_siren = {}
    for dirigeant in list_dict_dirigeants:
        siren = f'{dirigeant["siren"]}'
        if siren in dirigeants_by_siren:
            dirigeants_by_siren[siren].append(dirigeant)
        else:
            dirigeants_by_siren[siren] = [dirigeant]

    list_dirigeants_unique = []
    for key_dirigeant, same_dirigeants in dirigeants_by_siren.items():
        first_dirigeant = same_dirigeants[0]
        if len(same_dirigeants) == 1:
            list_dirigeants_unique.append(first_dirigeant)
        else:
            unique_dirigeant = {
                "siren": first_dirigeant["siren"],
                "denomination": first_dirigeant["denomination"],
                "sigle": first_dirigeant["sigle"],
                "qualite": unique_qualites(
                    ", ".join(
                        [dirigeant.get("qualite", "") for dirigeant in same_dirigeants]
                    )
                ),
            }
            list_dirigeants_unique.append(unique_dirigeant)
    return list_dirigeants_unique
