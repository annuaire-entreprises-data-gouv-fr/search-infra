def get_elasticsearch_field_name(param_name: str) -> str:
    corresponding_es_field = {
        "liste_idcc": "convention_collective_renseignee",
        "liste_finess": "est_finess",
        "liste_id_bio": "est_bio",
        "liste_rge": "est_rge",
        "liste_uai": "est_uai",
    }
    if param_name in corresponding_es_field:
        return corresponding_es_field[param_name]
    return param_name
