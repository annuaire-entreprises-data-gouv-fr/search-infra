from dag_datalake_sirene.colors import NEXT_COLOR
from elasticsearch_dsl import (
    Boolean,
    Date,
    Document,
    GeoPoint,
    Integer,
    Keyword,
    Object,
    Text,
    analyzer,
    token_filter,
    tokenizer,
)

# Define filters
french_elision = token_filter(
    "french_elision",
    type="elision",
    articles_case=True,
    articles=[
        "l",
        "m",
        "t",
        "qu",
        "n",
        "s",
        "j",
        "d",
        "c",
        "jusqu",
        "quoiqu",
        "lorsqu",
        "puisqu",
    ],
)
french_stop = token_filter("french_stop", type="stop", stopwords="_french_")
french_stemmer = token_filter("french_stemmer", type="stemmer", language="light_french")
# ignore_case option deprecated, use lowercase filter before synonym filter
french_synonym = token_filter(
    "french_synonym", type="synonym", expand=True, synonyms=[]
)

# Define analyzer
annuaire_analyzer = analyzer(
    "annuaire_analyzer",
    tokenizer=tokenizer("icu_tokenizer"),
    filter=[
        "lowercase",
        french_elision,
        french_stop,
        "icu_folding",
        french_synonym,
        "asciifolding",
        french_stemmer,
    ],
)


class ElasticsearchSireneIndex(Document):
    """

    Model-like class for persisting documents in elasticsearch.
    It's a wrapper around document to create specific mappings and to add settings in
    elasticsearch.

    Class used to represent a company headquarters,
    one siren number and the corresponding headquarters siret number

    """

    activite_principale_siege = Keyword()  # Add index_prefixes option
    activite_principale_unite_legale = Keyword()
    activite_principale_registre_metier = Keyword()
    adresse_etablissement = Text()
    adresse_etablissement_2 = Text()
    categorie_entreprise = Text()
    cedex = Keyword()
    code_pays_etranger = Text()
    code_postal = Keyword()
    commune = Keyword()
    complement_adresse = Text()
    concat_enseigne_adresse = Text(analyzer=annuaire_analyzer)
    concat_nom_adr_siren = Text(
        analyzer=annuaire_analyzer, fields={"keyword": Keyword()}
    )
    coordonnees = GeoPoint()
    date_creation_siege = Date()
    date_creation_unite_legale = Date()
    date_debut_activite_siege = Date()
    date_mise_a_jour = Date()
    departement = Keyword()
    dirigeants_pp = Object(
        properties={'siren': Text(), 'noms': Text(), 'prenoms': Text(),
                    'date_naissance': Date(), 'ville_naissance': Text(),
                    'pays_naissance': Text(), 'qualite': Text()})
    dirigeants_pm = Object(
        properties={'siren': Text(), 'denomination': Text(), 'qualite': Text()})
    distribution_speciale = Text()
    economie_sociale_solidaire_unite_legale = Keyword()
    enseigne = Text()
    etat_administratif_unite_legale = Keyword()
    etat_administratif_siege = Keyword()
    geo_adresse = Text(analyzer=annuaire_analyzer)
    geo_id = Keyword()
    identifiant_association_unite_legale = Keyword()
    indice_repetition = Text()
    is_entrepreneur_individuel = Boolean()
    is_siege = Boolean()
    latitude = Text()
    libelle_cedex = Text()
    libelle_commune = Text()
    libelle_commune_etranger = Text()
    libelle_pays_etranger = Text()
    libelle_voie = Text()
    liste_adresse = Text(analyzer=annuaire_analyzer)
    liste_dirigeants = Text()
    liste_enseigne = Text(analyzer=annuaire_analyzer)
    longitude = Text()
    nature_juridique_unite_legale = Integer()
    nom = Text()
    nom_complet = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    nom_raison_sociale = Text()
    nombre_etablissements = Integer()  # NaN can't be stored in an integer array
    nombre_etablissements_ouverts = Integer()
    numero_voie = Text()
    prenom = Keyword()
    section_activite_principale = Keyword()
    sigle = Keyword()
    siren = Keyword(required=True)
    siret_siege = Keyword(required=True)
    type_voie = Text()
    tranche_effectif_salarie_siege = Keyword()
    tranche_effectif_salarie_unite_legale = Keyword()

    class Index:
        name = f"siren-{NEXT_COLOR}"
        settings = {"number_of_shards": 1,
                    "number_of_replicas": 0,
                    'mapping': {'ignore_malformed': True}}
