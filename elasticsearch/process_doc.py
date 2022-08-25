import json

from dag_datalake_sirene.data_enrichment import (
    create_adresse_complete,
    create_coordonnees,
    create_departement,
    create_entrepreneur_individuel,
    create_nom_complet,
    create_section,
)
from dag_datalake_sirene.helpers.single_dispatch_funcs import get_string


def process_doc(res):
    arr = []
    for result in res:
        doc = {}
        for item in result:
            if item in ["enseignes", "adresses"]:
                doc[item] = json.loads(result[item])
            else:
                doc[item] = result[item]

        # Enseignes
        doc["liste_enseignes"] = []
        for enseigne in doc["enseignes"]:
            doc["liste_enseignes"].extend(
                [enseigne["enseigne_1"], enseigne["enseigne_2"], enseigne["enseigne_3"]]
            )
        doc["liste_enseignes"] = list(set(filter(None, doc["liste_enseignes"])))
        del doc["enseignes"]

        # Adresses
        doc["liste_adresses"] = []
        for adresse in doc["adresses"]:
            doc["liste_adresses"].append(
                create_adresse_complete(
                    adresse["complement_adresse"],
                    adresse["numero_voie"],
                    adresse["indice_repetition"],
                    adresse["type_voie"],
                    adresse["libelle_voie"],
                    adresse["libelle_commune"],
                    adresse["libelle_cedex"],
                    adresse["distribution_speciale"],
                    adresse["commune"],
                    adresse["cedex"],
                    adresse["libelle_commune_etranger"],
                    adresse["libelle_pays_etranger"],
                )
            )
        doc["liste_adresses"] = list(set(filter(None, doc["liste_adresses"])))
        del doc["adresses"]

        doc["adresse_etablissement"] = create_adresse_complete(
            doc["complement_adresse"],
            doc["numero_voie"],
            doc["indice_repetition"],
            doc["type_voie"],
            doc["libelle_voie"],
            doc["libelle_commune"],
            doc["libelle_cedex"],
            doc["distribution_speciale"],
            doc["commune"],
            doc["cedex"],
            doc["libelle_commune_etranger"],
            doc["libelle_pays_etranger"],
        )

        doc["nom_complet"] = create_nom_complet(
            result["nature_juridique_unite_legale"],
            result["nom"],
            result["nom_usage"],
            result["nom_raison_sociale"],
            result["sigle"],
            result["prenom"],
        )

        doc['liste_dirigeants'] = []

        doc['dirigeants_pp'] = json.loads(result['dirigeants_pp'])
        for dirigeant_pp in doc['dirigeants_pp']:
            doc['liste_dirigeants'].append(
                dirigeant_pp["prenoms"] + " " + dirigeant_pp["noms"])

        doc['dirigeants_pm'] = json.loads(result['dirigeants_pm'])
        for dirigeant_pm in doc['dirigeants_pm']:
            doc['liste_dirigeants'].append(dirigeant_pm["denomination"])

        doc["is_entrepreneur_individuel"] = create_entrepreneur_individuel(
            result["nature_juridique_unite_legale"]
        )
        doc["section_activite_principale"] = create_section(
            result["activite_principale_unite_legale"]
        )
        doc["departement"] = create_departement(result["commune"])
        doc["coordonnees"] = create_coordonnees(result["longitude"], result["latitude"])
        doc["concat_enseigne_adresse"] = doc["liste_enseignes"] + doc["liste_adresses"]

        doc["concat_nom_adr_siren"] = (
            get_string(doc["nom_complet"])
            + " "
            + get_string(doc["adresse_etablissement"])
            + " "
            + get_string(result["siren"])
        ).strip()
        arr.append(doc)
    return arr
