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

from psutil import virtual_memory, swap_memory

def mem():
    print(f'swap memory : {round(swap_memory()[1]/(1024*1024*1024)*10)/10}Go')
    print(f'used memory : {round(virtual_memory()[3]/(1024*1024*1024)*10)/10}Go')


# Upload geo data by departement

# we can reduce this to only the download time by scrapping pandas entirey and only saving csv on disk
# I didnot do it as it requires to rewrite column names
# should save ~ 4~5min

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

# Get geo data file paths
geo_files = glob.glob(DATA_DIR + "geo_siret*.csv")

geo_files_filtered = [DATA_DIR+"geo_siret_"+dep+".csv" for dep in all_deps]
geo_files_filtered

def adresse_complete(cols, row, adresse_2=False):
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
    get = lambda x: row[cols[x]]

    siren = get('siren')
    unite_legale = get_key(siren, all_unite_legale, [
        siren,
        0, # nombre_etablissements
        0, # nombre_etablissements_ouverts
        [],
        '', # adresse_complete
        '', # adresse_complete_2
    ])
    
    siret = get('siret')
    enseigne = ''.join([
                    get("enseigne_1"),
                    get("enseigne_2"),
                    get("enseigne_3"),
                    get("nom_commercial"),
                ])

    adresse = get("geo_adresse")
    isSiege = get('is_siege')
    
    etablissement = [
        siret,
        # enseigne,
        # adresse
    ]
    
    unite_legale[3].append(etablissement)
    unite_legale[1] = unite_legale[1] + 1
    
    if isSiege:
        unite_legale[4] = str.encode(adresse_complete(cols, row))
        unite_legale[5] = str.encode(adresse_complete(cols, row, adresse_2=True))
    
    is_etablissement_ouvert = get("etat_administratif_etablissement") == "A"
    
    if is_etablissement_ouvert: 
        unite_legale[2] = unite_legale[2] + 1
    
    all_unite_legale[siren] = unite_legale


def main ():
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


if __name__ == '__main__':
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
