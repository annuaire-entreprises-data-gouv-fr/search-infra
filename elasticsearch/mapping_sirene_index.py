from dag_datalake_sirene.colors import NEXT_COLOR
from elasticsearch_dsl import (
    Boolean,
    Date,
    Document,
    GeoPoint,
    InnerDoc,
    Integer,
    Keyword,
    Nested,
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

ELASTIC_SHARDS = 4


class ElasticsearchDirigeantPPIndex(InnerDoc):
    nom = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    prenoms = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    date_naissance = Date()
    ville_naissance = Text()
    pays_naissance = Text()
    qualite = Text(analyzer=annuaire_analyzer)


class ElasticsearchDirigeantPMIndex(InnerDoc):
    siren = Keyword()
    denomination = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    sigle = Text(analyzer=annuaire_analyzer)
    qualite = Text(analyzer=annuaire_analyzer)


class ElasticsearchEtablissementIndex(InnerDoc):
    activite_principale = Text()
    activite_principale_registre_metier = Keyword()
    adresse = Text(analyzer=annuaire_analyzer)
    cedex = Keyword()
    cedex_2 = Text()
    code_pays_etranger = Text()
    code_pays_etranger_2 = Text()
    code_postal = Keyword()
    commune = Keyword()
    commune_2 = Text()
    concat_nom_enseigne_adresse_siren_siret = Text(
        analyzer=annuaire_analyzer, fields={"keyword": Keyword()}
    )
    coordonnees = GeoPoint()
    complement_adresse = Text()
    complement_adresse_2 = Text()
    date_creation = Date()
    date_debut_activite = Date()
    departement = Keyword()
    distribution_speciale = Text()
    distribution_speciale_2 = Text()
    enseigne_1 = Text(analyzer=annuaire_analyzer)
    enseigne_2 = Text(analyzer=annuaire_analyzer)
    enseigne_3 = Text(analyzer=annuaire_analyzer)
    est_siege = Boolean()
    etat_administratif = Keyword()
    geo_adresse = Text(analyzer=annuaire_analyzer)
    geo_id = Keyword()
    indice_repetition = Text()
    indice_repetition_2 = Text()
    latitude = Text()
    liste_finess = Text()
    liste_idcc = Text()
    liste_rge = Text()
    liste_uai = Text()
    libelle_cedex = Text()
    libelle_cedex_2 = Text()
    libelle_commune = Text()
    libelle_commune_2 = Text()
    libelle_commune_etranger = Text()
    libelle_commune_etranger_2 = Text()
    libelle_pays_etranger = Text()
    libelle_pays_etranger_2 = Text()
    libelle_voie = Text()
    libelle_voie_2 = Text()
    longitude = Text()
    nom_commercial = Text()
    numero_voie = Text()
    numero_voie_2 = Text()
    siren = Keyword(required=True)
    siret = Keyword(required=True)
    tranche_effectif_salarie = Keyword()
    type_voie = Text()
    type_voie_2 = Text()


class ElasticsearchEluIndex(InnerDoc):
    nom = Text()
    prenom = Text()
    date_naissance = Date()
    sexe = Text()
    fonction = Text()


class ElasticsearchSireneIndex(Document):
    """

    Model-like class for persisting documents in elasticsearch.
    It's a wrapper around Document to create specific mappings and to add settings in
    elasticsearch.

    Class used to represent a company headquarters,
    one siren number and the corresponding headquarters siret number

    """

    activite_principale_unite_legale = Keyword()
    categorie_entreprise = Text()
    colter_code = Keyword()
    colter_code_insee = Keyword()
    colter_elus = Nested(ElasticsearchEluIndex)
    colter_niveau = Keyword()
    date_creation_unite_legale = Date()
    date_mise_a_jour_unite_legale = Date()
    dirigeants_pp = Nested(ElasticsearchDirigeantPPIndex)
    dirigeants_pm = Nested(ElasticsearchDirigeantPMIndex)
    economie_sociale_solidaire_unite_legale = Keyword()
    est_entrepreneur_individuel = Boolean()
    est_entrepreneur_spectacle = Boolean()
    etablissements = Nested(ElasticsearchEtablissementIndex)
    etat_administratif_unite_legale = Keyword()
    identifiant_association_unite_legale = Keyword()
    liste_dirigeants = Text(analyzer=annuaire_analyzer)
    liste_elus = Text(analyzer=annuaire_analyzer)
    nature_juridique_unite_legale = Integer()
    nom = Text(analyzer=annuaire_analyzer)
    nom_complet = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    nom_raison_sociale = Text()
    nombre_etablissements = Integer()  # NaN can't be stored in an integer array
    nombre_etablissements_ouverts = Integer()
    prenom = Text(analyzer=annuaire_analyzer)
    section_activite_principale = Keyword()
    sigle = Keyword()
    siren = Keyword(required=True)
    siret_siege = Keyword()
    tranche_effectif_salarie_unite_legale = Keyword()

    class Index:
        name = f"siren-{NEXT_COLOR}"
        settings = {
            "number_of_shards": ELASTIC_SHARDS,
            "number_of_replicas": 0,
            "mapping": {"ignore_malformed": True},
            "index.mapping.nested_objects.limit": 20000,
        }
