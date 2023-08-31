from dag_datalake_sirene.elasticsearch.data_enrichment import (
    format_adresse_complete,
    format_slug_association,
)


def process_association(chunk_associations_sqlite):
    list_associations_processed = []
    for association in chunk_associations_sqlite:
        association_processed = {}
        for field in association:
            association_processed[field] = association[field]

        # Create association (structure) to be indexed
        association_to_index = {}
        association_to_index["identifiant"] = association_processed[
            "identifiant_association"
        ]
        association_to_index["nom_complet"] = association_processed["titre"]
        association_to_index["adresse"] = format_adresse_complete(
            complement_adresse=association_processed["complement_adresse"],
            numero_voie=association_processed["numero_voie"],
            indice_repetition=association_processed["indice_repetition"],
            type_voie=association_processed["type_voie"],
            libelle_voie=association_processed["libelle_voie"],
            libelle_commune=association_processed["libelle_commune"],
            distribution_speciale=association_processed["distribution_speciale"],
            code_postal=association_processed["code_postal"],
            commune=association_processed["commune"],
        )
        association_to_index["est_association"] = True
        association_to_index["slug"] = format_slug_association(
            association_processed["titre"],
            association_processed["identifiant_association"],
        )
        association_to_index["association"] = association_processed
        list_associations_processed.append(association_to_index)

    return list_associations_processed
