from dag_datalake_sirene.data_enrichment import create_nom_complet, \
    create_departement, create_coordonnees, create_section, \
    create_entrepreneur_individuel
from dag_datalake_sirene.helpers.single_dispatch_funcs import unique_string, get_string
import logging
from elasticsearch import helpers
from dag_datalake_sirene.elasticsearch.mapping_siren import Siren


def process_res(res):
    arr = []
    for result in res:
        mydict = {}
        # mydict['siege'] = {}
        for item in result:
            # clean string from spaces
            if item == "liste_enseignes" or item == "liste_adresses":
                mydict[item] = unique_string(result[item])
            else:
                mydict[item] = result[item]

        mydict['nom_complet'] = create_nom_complet(
            result['nature_juridique_unite_legale'],
            result['nom'],
            result['nom_usage'],
            result['nom_raison_sociale'],
            result['sigle'],
            result['prenom'],
        )
        mydict['is_entrepreneur_individuel'] = create_entrepreneur_individuel(
            result['nature_juridique_unite_legale']
        )
        mydict['section_activite_principale'] = create_section(
            result['activite_principale_unite_legale']
        )
        mydict['departement'] = create_departement(
            result['commune']
        )
        mydict['coordonnees'] = create_coordonnees(
            result['longitude'],
            result['latitude']
        )
        mydict['concat_enseigne_adresse'] = (get_string(
            unique_string(result['liste_enseignes'])) + " " + get_string(
            unique_string(result['liste_adresses']))).strip()
        mydict['concat_nom_adr_siren'] =\
            (get_string(mydict['nom_complet'])
             + " " + get_string(result['adresse_complete']) + " " + get_string(
                        result['siren'])).strip()
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
