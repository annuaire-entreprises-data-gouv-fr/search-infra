from elasticsearch_dsl import (
    Boolean,
    Date,
    Document,
    GeoPoint,
    InnerDoc,
    Integer,
    Keyword,
    Long,
    Nested,
    Object,
    Text,
    analyzer,
    char_filter,
    token_filter,
    tokenizer,
)
from dag_datalake_sirene.config import ELASTIC_SHARDS, ELASTIC_REPLICAS

# Define filters
french_stop = token_filter("french_stop", type="stop", stopwords="_french_")
french_stemmer = token_filter(
    "french_stemmer", type="stemmer", language="minimal_french"
)
# Ignore_case option deprecated, use lowercase filter before synonym filter
french_synonym = token_filter(
    "french_synonym", type="synonym", expand=True, synonyms=[]
)

# This filter replaces single quotes, dots and commas with empty char
# This char filter addition allows for finding queries which contain special characters
# without having to include the latter in the query
# e.g. "RENN'OV", "R.P.S.", "NUMERI'CITE"
remove_special_char = char_filter(
    "remove_special_char",
    type="mapping",
    mappings=["'=>", "‘=>", "`=>", "’=>", ",=>", ".=>"],
)

# This char filter replaces the French elision token filter.
# Character filters are executed within the analyzer first before token filters.
# Consequently, the character filter (above) which removes single quotes, among other
# special characters, would be executed first (before the elision token filter) and
# might disturb the elision filter execution in some cases.
# We replace the elision token filter with a pattern replace character filter
# example : L'EDUCATION
# * using elision token filter + apostrophe char filter -> leducation
# * using pattern replace char filter to remove "l'" +
# mapping char filter to remove apostrophe -> education

remove_elision_char = char_filter(
    "remove_elision_char",
    type="pattern_replace",
    pattern="(\\bl|\\bm|\\bt|\\bqu|\\bn|\\bs|\\bj|\\bd|\\bc|\\bjusqu|\\bquoiqu"
    "|\\blorsqu|\\bpuisqu)("
    "'|’|`|‘)",
    replacement="",
    flags="CASE_INSENSITIVE",
)

# Define analyzer
annuaire_analyzer = analyzer(
    "annuaire_analyzer",
    tokenizer=tokenizer("icu_tokenizer"),
    filter=[
        "lowercase",
        french_stop,
        "icu_folding",
        french_synonym,
        "asciifolding",
        french_stemmer,
    ],
    char_filter=[remove_elision_char, remove_special_char],
)


class DirigeantPPMapping(InnerDoc):
    # Indexing the field 'nom' as both a keyword (exactly how it is given to the index)
    # and as text (analysed with the french analyser), allows us to search both the
    # exact match for a query and an analysed version of it (without stop words for
    # example)
    nom = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    nom_usage = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    prenoms = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    date_de_naissance = Date()
    nationalite = Text()
    role = Text(analyzer=annuaire_analyzer)
    date_mise_a_jour = Date()


class DirigeantPMMapping(InnerDoc):
    siren = Keyword()
    denomination = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    role = Text(analyzer=annuaire_analyzer)
    forme_juridique = Keyword()
    date_mise_a_jour = Date()


class BeneficiaireEffectifMapping(InnerDoc):
    # Indexing the field 'nom' as both a keyword (exactly how it is given to the index)
    # and as text (analysed with the french analyser), allows us to search both the
    # exact match for a query and an analysed version of it (without stop words for
    # example)
    nom = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    nom_usage = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    prenoms = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    date_de_naissance = Date()
    nationalite = Text()
    role = Text(analyzer=annuaire_analyzer)
    date_mise_a_jour = Date()


class EtablissementMapping(InnerDoc):
    activite_principale = Text()
    activite_principale_registre_metier = Keyword()
    adresse = Text(analyzer=annuaire_analyzer)
    annee_tranche_effectif_salarie = Date()
    caractere_employeur = Keyword()
    cedex = Keyword()
    cedex_2 = Text()
    code_pays_etranger = Text()
    code_pays_etranger_2 = Text()
    code_postal = Text(analyzer=annuaire_analyzer)
    # Using analyzer to be able to search using multi-match
    commune = Text(analyzer=annuaire_analyzer)
    commune_2 = Text()
    concat_enseigne_adresse_siren_siret = Text(
        analyzer=annuaire_analyzer, fields={"keyword": Keyword()}
    )
    coordonnees = GeoPoint()
    complement_adresse = Text()
    complement_adresse_2 = Text()
    date_creation = Date()
    date_debut_activite = Date()
    date_fermeture = Date()
    date_mise_a_jour_insee = Date()
    departement = Keyword()
    distribution_speciale = Text()
    distribution_speciale_2 = Text()
    enseigne_1 = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    enseigne_2 = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    enseigne_3 = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    epci = Keyword()
    est_siege = Boolean()
    etat_administratif = Keyword()
    geo_adresse = Text(analyzer=annuaire_analyzer)
    geo_id = Keyword()
    geo_score = Keyword()
    indice_repetition = Text()
    indice_repetition_2 = Text()
    latitude = Text()
    liste_finess = Text()
    liste_id_bio = Text()
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
    nom_commercial = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    nom_complet = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    numero_voie = Text()
    numero_voie_2 = Text()
    region = Keyword()
    siren = Keyword(required=True)
    siret = Keyword(required=True)
    statut_diffusion_etablissement = Keyword()
    tranche_effectif_salarie = Keyword()
    type_voie = Text()
    type_voie_2 = Text()
    x = Keyword()
    y = Keyword()


class SiegeMapping(InnerDoc):
    activite_principale = Text()
    activite_principale_registre_metier = Keyword()
    adresse = Text(analyzer=annuaire_analyzer)
    annee_tranche_effectif_salarie = Date()
    caractere_employeur = Keyword()
    cedex = Keyword()
    cedex_2 = Text()
    code_pays_etranger = Text()
    code_pays_etranger_2 = Text()
    code_postal = Keyword()
    commune = Keyword()
    commune_2 = Text()
    coordonnees = GeoPoint()
    complement_adresse = Text()
    complement_adresse_2 = Text()
    date_creation = Date()
    date_debut_activite = Date()
    date_fermeture = Date()
    date_mise_a_jour = Date()
    date_mise_a_jour_insee = Date()
    date_mise_a_jour_rne = Date()
    departement = Keyword()
    distribution_speciale = Text()
    distribution_speciale_2 = Text()
    enseigne_1 = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    enseigne_2 = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    enseigne_3 = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    epci = Keyword()
    est_siege = Boolean()
    etat_administratif = Keyword()
    geo_adresse = Text(analyzer=annuaire_analyzer)
    geo_id = Keyword()
    geo_score = Keyword()
    indice_repetition = Text()
    indice_repetition_2 = Text()
    latitude = Text()
    liste_finess = Text()
    liste_id_bio = Text()
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
    nom_commercial = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    numero_voie = Text()
    numero_voie_2 = Text()
    region = Keyword()
    siren = Keyword(required=True)
    siret = Keyword(required=True)
    tranche_effectif_salarie = Keyword()
    type_voie = Text()
    type_voie_2 = Text()
    x = Keyword()
    y = Keyword()


class EluMapping(InnerDoc):
    nom = Text()
    prenom = Text()
    date_naissance = Date()
    sexe = Text()
    fonction = Text()


class BilanFinancierMapping(InnerDoc):
    ca = Long()
    resultat_net = Long()
    date_cloture_exercice = Text()
    annee_cloture_exercice = Text()


class ImmatriculationMapping(InnerDoc):
    date_immatriculation = Date()
    date_radiation = Date()
    indicateur_associe_unique = Boolean()
    capital_social = Long()
    date_cloture_exercice = Text()
    duree_personne_morale = Integer()
    nature_entreprise = Text()
    date_debut_activite = Text()
    capital_variable = Boolean()
    devise_capital = Keyword()


class UniteLegaleMapping(InnerDoc):
    """

    Model-like class for persisting documents in elasticsearch.
    It's a wrapper around Document to create specific mappings and to add settings in
    elasticsearch.

    Class used to represent a company headquarters,
    one siren number and the corresponding headquarters siret number

    """

    activite_principale_unite_legale = Keyword()
    annee_categorie_entreprise = Date()
    annee_tranche_effectif_salarie = Date()
    beneficiaires_effectifs = Nested(BeneficiaireEffectifMapping)
    bilan_financier = Nested(BilanFinancierMapping)
    caractere_employeur = Keyword()
    code_categorie_entreprise = Integer()
    categorie_entreprise = Keyword()
    convention_collective_renseignee = Boolean()
    colter_code = Keyword()
    colter_code_insee = Keyword()
    colter_elus = Nested(EluMapping)
    colter_niveau = Keyword()
    date_creation_unite_legale = Date()
    date_fermeture = Date()
    date_mise_a_jour_insee = Date()
    date_mise_a_jour_rne = Date()
    denomination_usuelle_1_unite_legale = Text(
        analyzer=annuaire_analyzer, fields={"keyword": Keyword()}
    )
    denomination_usuelle_2_unite_legale = Text(
        analyzer=annuaire_analyzer, fields={"keyword": Keyword()}
    )
    denomination_usuelle_3_unite_legale = Text(
        analyzer=annuaire_analyzer, fields={"keyword": Keyword()}
    )
    dirigeants_pp = Nested(DirigeantPPMapping)
    dirigeants_pm = Nested(DirigeantPMMapping)
    economie_sociale_solidaire_unite_legale = Keyword()
    est_entrepreneur_individuel = Boolean()
    est_entrepreneur_spectacle = Boolean()
    egapro_renseignee = Boolean()
    est_association = Boolean()
    est_finess = Boolean()
    est_bio = Boolean()
    est_ess = Boolean()
    est_organisme_formation = Boolean()
    liste_id_organisme_formation = Text()
    est_qualiopi = Boolean()
    est_rge = Boolean()
    est_service_public = Boolean()
    est_siae = Boolean()
    est_societe_mission = Keyword()
    est_uai = Boolean()
    etablissements = Nested(EtablissementMapping)
    etat_administratif_unite_legale = Keyword()
    facteur_taille_entreprise = Integer()
    from_insee = Boolean()
    from_rne = Boolean()
    identifiant_association_unite_legale = Keyword()
    immatriculation = Nested(ImmatriculationMapping)
    liste_beneficiaires = Text(analyzer=annuaire_analyzer)
    liste_dirigeants = Text(analyzer=annuaire_analyzer)
    liste_elus = Text(analyzer=annuaire_analyzer)
    liste_idcc = Text()
    liste_idcc_by_siren = Text()
    nature_juridique_unite_legale = Keyword()
    nom = Text(analyzer=annuaire_analyzer)
    nom_raison_sociale = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    nombre_etablissements = Integer()  # NaN can't be stored in an integer array
    nombre_etablissements_ouverts = Integer()
    prenom = Text(analyzer=annuaire_analyzer)
    section_activite_principale = Keyword()
    siege = Object(SiegeMapping)
    sigle = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    siren = Keyword(required=True)
    siret_siege = Keyword()
    slug = Text()
    statut_diffusion_unite_legale = Keyword()
    statut_entrepreneur_spectacle = Text()
    type_siae = Text()
    tranche_effectif_salarie_unite_legale = Keyword()


class StructureMapping(Document):
    identifiant = Keyword()
    nom_complet = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})
    adresse = Text(analyzer=annuaire_analyzer)
    unite_legale = Object(UniteLegaleMapping)

    class Index:
        settings = {
            "number_of_shards": ELASTIC_SHARDS,
            "number_of_replicas": ELASTIC_REPLICAS,
            "mapping": {"ignore_malformed": True},
            "index.mapping.nested_objects.limit": 20000,
        }
