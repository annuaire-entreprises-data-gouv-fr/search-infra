import time
import pandas as pd
import glob
import datetime
import os, shutil
import numpy as np
import swifter
import math
import json
from csv import reader
from os.path import exists
from psutil import virtual_memory, swap_memory
import gc

start_time = time.time()

if "DATA_DIR" not in locals():
    DATA_DIR = "./data/"
else:
    print(DATA_DIR)

if not os.path.exists(DATA_DIR) and os.path.isdir(DATA_DIR):
    os.makedirs(os.path.dirname(DATA_DIR), exist_ok=True)
    
if "OUTPUT_DATA_FOLDER" not in locals():
    OUTPUT_DATA_FOLDER = "./output/"
else:
    print(OUTPUT_DATA_FOLDER)

if os.path.exists(OUTPUT_DATA_FOLDER) and os.path.isdir(OUTPUT_DATA_FOLDER):
    shutil.rmtree(OUTPUT_DATA_FOLDER)
os.makedirs(os.path.dirname(OUTPUT_DATA_FOLDER), exist_ok=True)

if "LABELS_FOLDER" not in locals():
    LABELS_FOLDER = "./labels/"
else:
    print(LABELS_FOLDER)

if "ELASTIC_INDEX" not in locals():
    ELASTIC_INDEX = "siren"
else:
    print(ELASTIC_INDEX)

# Create list of departement zip codes
all_deps = [
    *"-0".join(list(str(x) for x in range(0, 10))).split("-")[1:],
    *list(str(x) for x in range(10, 20)),
    *["2A", "2B"],
    *list(str(x) for x in range(21, 96)),
    *"-7510".join(list(str(x) for x in range(0, 10))).split("-")[1:],
    *"-751".join(list(str(x) for x in range(10, 21))).split("-")[1:],
    *["971", "972", "973", "974", "976"],
    *[""],
]
# Remove Paris zip code
all_deps.remove("75")
# all_deps = ["13", "69", "59", "33", "06", "92", "93", "34", "83", "31","94","78","44","38","77","91","95","67","76"]

sections_NAF = {
"01":"A","02":"A","03":"A","05":"B","06":"B","07":"B","08":"B","09":"B","10":"C","11":"C","12":"C","13":"C","14":"C",
 "15":"C","16":"C","17":"C","18":"C","19":"C","20":"C","21":"C","22":"C","23":"C","24":"C","25":"C","26":"C","27":"C",
 "28":"C","29":"C","30":"C","31":"C","32":"C","33":"C","35":"D","36":"E","37":"E","38":"E","39":"E","41":"F","42":"F",
 "43":"F","45":"G","46":"G","47":"G","49":"H","50":"H","51":"H","52":"H","53":"H","55":"I","56":"I","58":"J","59":"J",
 "60":"J","61":"J","62":"J","63":"J","64":"K","65":"K","66":"K","68":"L","69":"M","70":"M","71":"M","72":"M","73":"M",
 "74":"M","75":"M","77":"N","78":"N","79":"N","80":"N","81":"N","82":"N","84":"O","85":"P","86":"Q","87":"Q","88":"Q",
 "90":"R","91":"R","92":"R","93":"R","94":"S","95":"S","96":"S","97":"T","98":"T","99":"U"
}


def mem():
    print(f'swap memory : {round(swap_memory()[1]/(1024*1024*1024)*10)/10}Go')
    print(f'used memory : {round(virtual_memory()[3]/(1024*1024*1024)*10)/10}Go')


# Etablissements by departement

for dep in all_deps:
    file_path = DATA_DIR + "geo_siret_" + dep + ".csv"
    
    file_exists = exists(file_path)
    
    if(file_exists):
        print("file already present : "+file_path)
        continue
    
    stats()
    url = "https://files.data.gouv.fr/geo-sirene/last/dep/geo_siret_" + dep + ".csv.gz"
    print(url)
    df_dep = pd.read_csv(
        url,
        compression="gzip",
        dtype=str,
        usecols=[
            "siren",
            "siret",
            "dateCreationEtablissement",
            "trancheEffectifsEtablissement",
            "activitePrincipaleRegistreMetiersEtablissement",
            "etablissementSiege",
            "numeroVoieEtablissement",
            "libelleVoieEtablissement",
            "codePostalEtablissement",
            "libelleCommuneEtablissement",
            "libelleCedexEtablissement",
            "typeVoieEtablissement",
            "codeCommuneEtablissement",
            "codeCedexEtablissement",
            "complementAdresseEtablissement",
            "distributionSpecialeEtablissement",
            "complementAdresse2Etablissement",
            "indiceRepetition2Etablissement",
            "libelleCedex2Etablissement",
            "codeCedex2Etablissement",
            "numeroVoie2Etablissement",
            "typeVoie2Etablissement",
            "libelleVoie2Etablissement",
            "codeCommune2Etablissement",
            "libelleCommune2Etablissement",
            "distributionSpeciale2Etablissement",
            "dateDebut",
            "etatAdministratifEtablissement",
            "enseigne1Etablissement",
            "enseigne1Etablissement",
            "enseigne2Etablissement",
            "enseigne3Etablissement",
            "denominationUsuelleEtablissement",
            "activitePrincipaleEtablissement",
            "geo_adresse",
            "geo_id",
            "longitude",
            "latitude",
            "indiceRepetitionEtablissement",
            "libelleCommuneEtrangerEtablissement",
            "codePaysEtrangerEtablissement",
            "libellePaysEtrangerEtablissement",
            "libelleCommuneEtranger2Etablissement",
            "codePaysEtranger2Etablissement",
            "libellePaysEtranger2Etablissement",
        ],
    )
    df_dep = df_dep.rename(
        columns={
            "dateCreationEtablissement": "date_creation",
            "trancheEffectifsEtablissement": "tranche_effectif_salarie",
            "activitePrincipaleRegistreMetiersEtablissement": "activite_principale_registre_metier",
            "etablissementSiege": "is_siege",
            "numeroVoieEtablissement": "numero_voie",
            "typeVoieEtablissement": "type_voie",
            "libelleVoieEtablissement": "libelle_voie",
            "codePostalEtablissement": "code_postal",
            "libelleCedexEtablissement": "libelle_cedex",
            "libelleCommuneEtablissement": "libelle_commune",
            "codeCommuneEtablissement": "commune",
            "complementAdresseEtablissement": "complement_adresse",
            "complementAdresse2Etablissement": "complement_adresse_2",
            "numeroVoie2Etablissement": "numero_voie_2",
            "indiceRepetition2Etablissement": "indice_repetition_2",
            "typeVoie2Etablissement": "type_voie_2",
            "libelleVoie2Etablissement": "libelle_voie_2",
            "codeCommune2Etablissement": "commune_2",
            "libelleCommune2Etablissement": "libelle_commune_2",
            "codeCedex2Etablissement": "cedex_2",
            "libelleCedex2Etablissement": "libelle_cedex_2",
            "codeCedexEtablissement": "cedex",
            "dateDebut": "date_debut_activite",
            "distributionSpecialeEtablissement": "distribution_speciale",
            "distributionSpeciale2Etablissement": "distribution_speciale_2",
            "etatAdministratifEtablissement": "etat_administratif_etablissement",
            "enseigne1Etablissement": "enseigne_1",
            "enseigne2Etablissement": "enseigne_2",
            "enseigne3Etablissement": "enseigne_3",
            "activitePrincipaleEtablissement": "activite_principale",
            "indiceRepetitionEtablissement": "indice_repetition",
            "denominationUsuelleEtablissement": "nom_commercial",
            "libelleCommuneEtrangerEtablissement": "libelle_commune_etranger",
            "codePaysEtrangerEtablissement": "code_pays_etranger",
            "libellePaysEtrangerEtablissement": "libelle_pays_etranger",
            "libelleCommuneEtranger2Etablissement": "libelle_commune_etranger_2",
            "codePaysEtranger2Etablissement": "code_pays_etranger_2",
            "libellePaysEtranger2Etablissement": "libelle_pays_etranger_2",
        }
    )
    df_dep.to_csv(file_path, index=False)


# UNITE LEGALE

unite_file = DATA_DIR + "unite_legales.csv"

unite_legale_file_exists = exists(unite_file)

if not unite_legale_file_exists:
    start_time = time.time()
    chunk = pd.read_csv(
        "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip",
        compression="zip",
        dtype=str,
        chunksize=100000,
        usecols=[
            "siren",
            "dateCreationUniteLegale",
            "sigleUniteLegale",
            "prenom1UniteLegale",
            "identifiantAssociationUniteLegale",
            "trancheEffectifsUniteLegale",
            "dateDernierTraitementUniteLegale",
            "categorieEntreprise",
            "etatAdministratifUniteLegale",
            "nomUniteLegale",
            "nomUsageUniteLegale",
            "denominationUniteLegale",
            "categorieJuridiqueUniteLegale",
            "activitePrincipaleUniteLegale",
            "economieSocialeSolidaireUniteLegale",
        ],
    )
    # Rename columns
    df_unite_legale = pd.concat(chunk)
    df_unite_legale = df_unite_legale.rename(
        columns={
            "dateCreationUniteLegale": "date_creation_unite_legale",
            "sigleUniteLegale": "sigle",
            "prenom1UniteLegale": "prenom",
            "trancheEffectifsUniteLegale": "tranche_effectif_salarie_unite_legale",
            "dateDernierTraitementUniteLegale": "date_mise_a_jour_unite_legale",
            "categorieEntreprise": "categorie_entreprise",
            "etatAdministratifUniteLegale":"etat_administratif_unite_legale",
            "nomUniteLegale": "nom",
            "nomUsageUniteLegale": "nom_usage",
            "denominationUniteLegale": "nom_raison_sociale",
            "categorieJuridiqueUniteLegale": "nature_juridique_unite_legale",
            "activitePrincipaleUniteLegale": "activite_principale_unite_legale",
            "economieSocialeSolidaireUniteLegale":"economie_sociale_solidaire_unite_legale",
            "identifiantAssociationUniteLegale":"identifiant_association_unite_legale",
        }
    )
    df_unite_legale.to_csv(unite_file, index=False)

def get_adresse_complete(cols, row, adresse_2=False):
    def get(x, default=None): 
        val = row[cols[x]]
        if not val:
            return default
        return val
    
    col_list = ["complement_adresse", "numero_voie", "indice_repetition", "type_voie", "libelle_voie", "distribution_speciale"]
    adresse = ""
    for column in col_list:
        col_label = f'{column}_2' if adresse_2 else column
        
        adresse = adresse + (" " + str(get(column)) if get(column) else "")
        
    if get("cedex") is None:
        if get("commune") is None:
            adresse =  adresse
        else:
            adresse = adresse + " " + str(get("commune")) + " " + str(get("libelle_commune"))
    else:
        adresse = adresse + " " + str(get("cedex")) + " " + str(get("libelle_cedex"))
    etranger_list = ["libelle_commune_etranger", "libelle_pays_etranger"]
    for column in etranger_list:
        adresse = adresse + (" " + str(get(column)) if get(column) else "")
    return adresse.strip()

def get_key(k, dico, default=0):
    if(k in dico):
        return dico[k]
    
    return default

    
def parse_etab(cols, row, all_unite_legale, index):
    def get(x, default=None): 
        val = row[cols[x]]
        if not val:
            return default
        return val

    siren = get('siren')
    unite_legale = get_key(siren, all_unite_legale, {
        'siren':siren,
        'nombre_etablissements_ouverts':0,
        'nombre_etablissements':0,
        "etablissements":[]
    })
        
    etat_administratif_etablissement = get("etat_administratif_etablissement")
    date_creation = get("date_creation")
    tranche_effectif_salarie = get("tranche_effectif_salarie")
    date_debut_activite = get("date_debut_activite")
    activite_principale = get("activite_principale")
    numero_voie = get("numero_voie")
    type_voie = get("type_voie")
    libelle_voie = get("libelle_voie")
    distribution_speciale = get("distribution_speciale")
    cedex = get("cedex")
    libelle_cedex = get("libelle_cedex")
    commune = get("commune")
    libelle_commune = get("libelle_commune")
    code_pays_etranger = get("code_pays_etranger")
    code_postal = get("code_postal")
    geo_id = get("geo_id")
    longitude = get("longitude")
    latitude = get("latitude")
    activite_principale_registre_metier = get("activite_principale_registre_metier")
     
    siret = get('siret')
    enseigne = ''.join([
                    get("enseigne_1", ""),
                    get("enseigne_2", ""),
                    get("enseigne_3", ""),
                    get("nom_commercial", ""),
                ]).strip() or None

    is_siege = get('is_siege')
    
    etablissement = [
        siret,
        enseigne,
        is_siege,
        etat_administratif_etablissement,
        date_creation,
        tranche_effectif_salarie,
        date_debut_activite,
        activite_principale,
        numero_voie,
        type_voie,
        libelle_voie,
        distribution_speciale,
        cedex,
        libelle_cedex,
        commune,
        libelle_commune,
        code_pays_etranger,
        code_postal,
        geo_id,
        longitude,
        latitude,
        activite_principale_registre_metier,
    ]
    
    unite_legale['etablissements'].append(etablissement)
    unite_legale["nombre_etablissements"] = unite_legale["nombre_etablissements"] + 1

    is_etablissement_ouvert = etat_administratif_etablissement == "A"
    
    if is_etablissement_ouvert: 
        unite_legale["nombre_etablissements_ouverts"] = unite_legale["nombre_etablissements_ouverts"] + 1
    
    all_unite_legale[siren] = unite_legale

def nom_complet(cols, row):
    def get(x, default=None): 
        val = row[cols[x]]
        if not val:
            return default
        return val
    
    is_auto_entrepreneur = get("nature_juridique_unite_legale") == "1000"
    
    sigle = get("sigle")
    
    if is_auto_entrepreneur:
        prenom = get("prenom")
        nom = get("nom")
        nom_usage = get("nom_usage", " ")
        formatted_nom_usage = " " + nom_usage.lower() if nom_usage else ""
        formatted_sigle = ", "+ sigle if sigle else ""
        
        if (prenom is None and nom is None):
            return None
        else:
            return f'{prenom}{formatted_nom_usage} ({nom}{formatted_sigle})'.lower()
    else:
        nom_raison_sociale = get("nom_raison_sociale")
        
        if nom_raison_sociale is None and sigle is None:
            return None
        else:
            formatted_sigle = f' ({sigle})' if sigle else ""
            return f'{nom_raison_sociale}{formatted_sigle}'.lower()

def parse_unite(cols, row, all_unite_legale, index):
    def get(x, default=None): 
        val = row[cols[x]]
        if not val:
            return default
        return val

    siren = get('siren')
    unite_legale = get_key(siren, all_unite_legale, {
        'siren':siren,
        'nombre_etablissements_ouverts':0,
        'nombre_etablissements':0,
        "etablissements":[]
    })

    

    col_list = ["date_creation_unite_legale", "sigle", "prenom", "tranche_effectif_salarie_unite_legale", "date_mise_a_jour_unite_legale", "categorie_entreprise",
                "etat_administratif_unite_legale", "nom", "nom_usage", "nom_raison_sociale", "nature_juridique_unite_legale", "activite_principale_unite_legale",
                "economie_sociale_solidaire_unite_legale", "identifiant_association_unite_legale", "activite_principale_unite_legale"]
    
    for col in col_list:
        unite_legale[col] = get(col)

    all_unite_legale[siren] = unite_legale

def main ():
    geo_files_filtered = [DATA_DIR+"geo_siret_"+dep+".csv" for dep in all_deps]

    all_unite_legale = {}
    total = len(geo_files_filtered)
    count= 0
    total_etab=0

    for geo_file in geo_files_filtered:
        count = count+1

        print("====")
        mem()
        print(str(count)+'/'+str(total))
        print("file : "+geo_file)
        
        cols = {}
        with open(geo_file, 'r') as read_obj:
            start_time = time.time()
            
            # pass the file object to reader() to get the reader object
            csv_reader = reader(read_obj)
            # Iterate over each row in the csv using reader object
            for index, row in enumerate(csv_reader):
                if(index==0):
                    cols = { val: index for (index, val) in enumerate(row)}
                else:
                    parse_etab(cols, row, all_unite_legale, index)
                last = index
            
            print("etablissements added : "+str(last))
            print("computed in : %s seconds" % (round(time.time() - start_time))) 
            total_etab = total_etab+last

    print(total_etab)
    print(len(all_unite_legale))

    print('process unite légale')

    start_time = time.time()
    with open(unite_file, 'r') as read_obj:
        cols = {}
        # pass the file object to reader() to get the reader object
        csv_reader = reader(read_obj)
        # Iterate over each row in the csv using reader object
        for index, row in enumerate(csv_reader):
            if(index==0):
                cols = { val: index for (index, val) in enumerate(row)}
            else:
                parse_unite(cols, row, all_unite_legale, index) 
    
    print("computed in : %s seconds" % (round(time.time() - start_time))) 
    print(len(all_unite_legale))

# def postTreatment():
#     for unite_legale in all_unite_legale.values():
#         unite_legale["liste_enseigne"] = list(filter(None, set(etab[1] for etab in unite_legale["etablissements"])))
#         unite_legale["liste_adresse"] = list(filter(None, set(etab[2] for etab in unite_legale["etablissements"])))
#         unite_legale["concat_enseigne_adresse"] = unite_legale["liste_enseigne"] + unite_legale["liste_adresse"]
#         activite_principale_unite_legale = get("activite_principale_unite_legale", '')
#         code_naf = activite_principale_unite_legale[:2]
#         unite_legale["nom_complet"] = nom_complet(cols, row)
#         unite_legale["section_activite_principale"] = sections_NAF[code_naf] if code_naf in sections_NAF else None
#         unite_legale["is_entrepreneur_individuel"] = True if unite_legale["nature_juridique_unite_legale"] in ['1', '10', '1000'] else False

#         departement = str(get("commune"))[:3] if str(get("commune"))[:2]== "97" else (None if get("commune") is None else str(get("commune"))[:2])
#         coordonnees = None if (longitude is None) or (latitude is None) else f'{latitude},{longitude}'
#         adresse_complete = get_adresse_complete(cols, row)

if __name__ == '__main__':
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
