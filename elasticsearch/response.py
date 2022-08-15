from dag_datalake_sirene.data_enrichment import create_nom_complet, \
    create_departement, create_coordonnees, create_section, \
    create_entrepreneur_individuel, create_adresse_complete
from dag_datalake_sirene.helpers.single_dispatch_funcs import unique_string, get_string
import logging
import json
from elasticsearch import helpers
from dag_datalake_sirene.elasticsearch.mapping_siren import Siren


def process_res(res):
    arr = []
    for result in res:
        mydict = {}
        for item in result:
            if item in ["enseignes", "adresses"]:
                mydict[item] = json.loads(result[item])
            else:
                mydict[item] = result[item]

        # enseignes
        mydict["liste_enseignes"] = []
        for enseigne in mydict["enseignes"]:
            mydict["liste_enseignes"].extend(
                [
                    enseigne["enseigne_1"],
                    enseigne["enseigne_2"],
                    enseigne["enseigne_3"]
                ]
            )
        mydict["liste_enseignes"] = list(set(filter(None, mydict["liste_enseignes"])))
        del mydict["enseignes"]

        # adresses
        mydict["liste_adresses"] = []
        for adresse in mydict["adresses"]:
            mydict["liste_adresses"].append(
                create_adresse_complete(
                    adresse["complement_adresse"], adresse["numero_voie"],
                    adresse["indice_repetition"], adresse["type_voie"],
                    adresse["libelle_voie"], adresse["libelle_commune"],
                    adresse["libelle_cedex"], adresse["distribution_speciale"],
                    adresse["commune"], adresse["cedex"],
                    adresse["libelle_commune_etranger"],
                    adresse["libelle_pays_etranger"]
                )
            )
        mydict["liste_adresses"] = list(set(filter(None, mydict["liste_adresses"])))
        del mydict["adresses"]

        mydict['adresse_etablissement'] = create_adresse_complete(
                    mydict["complement_adresse"], mydict["numero_voie"],
                    mydict["indice_repetition"], mydict["type_voie"],
                    mydict["libelle_voie"], mydict["libelle_commune"],
                    mydict["libelle_cedex"], mydict["distribution_speciale"],
                    mydict["commune"], mydict["cedex"],
                    mydict["libelle_commune_etranger"],
                    mydict["libelle_pays_etranger"]
                )

        mydict["nom_complet"] = create_nom_complet(
            result["nature_juridique_unite_legale"],
            result["nom"],
            result["nom_usage"],
            result["nom_raison_sociale"],
            result["sigle"],
            result["prenom"],
        )
        mydict["is_entrepreneur_individuel"] = create_entrepreneur_individuel(
            result["nature_juridique_unite_legale"]
        )
        mydict["section_activite_principale"] = create_section(
            result["activite_principale_unite_legale"]
        )
        mydict["departement"] = create_departement(
            result["commune"]
        )
        mydict["coordonnees"] = create_coordonnees(
            result["longitude"],
            result["latitude"]
        )
        mydict["concat_enseigne_adresse"] =\
            mydict["liste_enseignes"] + mydict["liste_adresses"]

        mydict["concat_nom_adr_siren"] = \
            (get_string(mydict["nom_complet"])
             + " " + get_string(mydict["adresse_etablissement"]) + " " + get_string(
                        result["siren"])).strip()
        arr.append(mydict)
    return arr


def doc_generator(data):
    for index, document in enumerate(data):
        yield Siren(meta={"id": document["siret_siege"]}, **document).to_dict(
            include_meta=True
        )
    # Serialize the instance into a dictionary so that it can be saved in elasticsearch.


def index_by_chunk(cursor, elastic_connection, elastic_bulk_size, elastic_index):
    i = 0
    res = 1
    while res:
        res = cursor.fetchmany(elastic_bulk_size)
        columns = tuple([x[0] for x in cursor.description])
        res = tuple(
            [{column: val for column, val in zip(columns, x)} for x in res]
        )
        res2 = process_res(res)
        i = i + 1
        if i % 1000 == 0:
            logging.info("i={i}")
        try:
            for success, details in helpers.parallel_bulk(elastic_connection,
                                                          doc_generator(res2),
                                                          chunk_size=elastic_bulk_size):
                if not success:
                    raise Exception(f"A file_access document failed: {details}")
        except Exception as e:
            logging.error(f"Failed to send to Elasticsearch: {e}")
        doc_count = elastic_connection.cat.count(
            index=elastic_index, params={"format": "json"}
        )[0]["count"]
        logging.info(f"Number of documents indexed: {doc_count}")
    return doc_count
