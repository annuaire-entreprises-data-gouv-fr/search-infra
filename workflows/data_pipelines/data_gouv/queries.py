ul_fields_to_select = """
SELECT unite_legale.etat_administratif_unite_legale as etat_administratif,
    unite_legale.statut_diffusion_unite_legale as statut_diffusion,
    colter.colter_code,
    colter.colter_code_insee,
    (
        SELECT json_group_array(
                json_object(
                    'siren',
                    siren,
                    'nom',
                    nom,
                    'prenom',
                    prenom,
                    'date_naissance',
                    date_naissance,
                    'sexe',
                    sexe,
                    'fonction',
                    fonction
                )
            )
        FROM (
                SELECT siren,
                    nom,
                    prenom,
                    date_naissance,
                    sexe,
                    fonction
                FROM elus
                WHERE siren = unite_legale.siren
            )
    ) as colter_elus,
    colter.colter_niveau,
    unite_legale.date_mise_a_jour_insee,
    unite_legale.date_mise_a_jour_rne,
    egapro.egapro_renseignee,
    achats_responsables.est_achats_responsables,
    alim_confiance.est_alim_confiance,
    patrimoine_vivant.est_patrimoine_vivant,
    unite_legale.economie_sociale_solidaire_unite_legale as economie_sociale_solidaire,
    spectacle.est_entrepreneur_spectacle,
    ess_france.est_ess_france,
    organisme_formation.est_qualiopi,
    marche_inclusion.est_siae,
    unite_legale.identifiant_association_unite_legale as identifiant_association,
    unite_legale.est_societe_mission,
    organisme_formation.liste_id_organisme_formation,
    (
        SELECT liste_idcc_unite_legale
        FROM convention_collective
        WHERE siren = unite_legale.siren
    ) as liste_idcc,
    unite_legale.nature_juridique_unite_legale as nature_juridique,
    unite_legale.nom,
    unite_legale.nom_raison_sociale,
    unite_legale.nom_usage,
    count_etablissement."count" as nombre_etablissements,
    count_etablissement_ouvert."count" as nombre_etablissements_ouverts,
    unite_legale.prenom,
    unite_legale.siren,
    siege.siret as siret_siege,
    unite_legale.sigle,
    spectacle.statut_entrepreneur_spectacle,
    marche_inclusion.type_siae,
    finess_juridique.liste_finess_juridique,
    aides_ademe.aide_ademe_renseignee,
    avocat.est_avocat
FROM unite_legale
    LEFT JOIN colter ON colter.siren = unite_legale.siren
    LEFT JOIN egapro ON egapro.siren = unite_legale.siren
    LEFT JOIN achats_responsables ON achats_responsables.siren = unite_legale.siren
    LEFT JOIN alim_confiance ON alim_confiance.siren = unite_legale.siren
    LEFT JOIN patrimoine_vivant ON patrimoine_vivant.siren = unite_legale.siren
    LEFT JOIN spectacle ON spectacle.siren = unite_legale.siren
    LEFT JOIN ess_france ON ess_france.siren = unite_legale.siren
    LEFT JOIN organisme_formation ON organisme_formation.siren = unite_legale.siren
    LEFT JOIN marche_inclusion ON marche_inclusion.siren = unite_legale.siren
    LEFT JOIN count_etablissement ON count_etablissement.siren = unite_legale.siren
    LEFT JOIN count_etablissement_ouvert ON count_etablissement_ouvert.siren = unite_legale.siren
    LEFT JOIN finess_juridique ON finess_juridique.siren = unite_legale.siren
    LEFT JOIN aides_ademe ON aides_ademe.siren = unite_legale.siren
    LEFT JOIN avocat ON avocat.siren = unite_legale.siren
    LEFT JOIN etablissement AS siege ON siege.siren = unite_legale.siren AND siege.est_siege = 'true'
WHERE unite_legale.siren IS NOT NULL;
"""


etab_fields_to_select = """SELECT etablissement.activite_principale,
    etablissement.activite_principale_registre_metier,
    etablissement.ancien_siege,
    etablissement.caractere_employeur,
    etablissement.cedex,
    etablissement.code_pays_etranger,
    etablissement.code_postal,
    etablissement.commune,
    etablissement.complement_adresse,
    etablissement.date_creation,
    etablissement.date_debut_activite,
    etablissement.date_fermeture_etablissement as date_fermeture,
    etablissement.distribution_speciale,
    etablissement.enseigne_1,
    etablissement.enseigne_2,
    etablissement.enseigne_3,
    etablissement.est_siege,
    etablissement.etat_administratif_etablissement as etat_administratif,
    etablissement.indice_repetition,
    etablissement.libelle_cedex,
    etablissement.libelle_commune,
    etablissement.libelle_commune_etranger,
    etablissement.libelle_pays_etranger,
    etablissement.libelle_voie,
    finess_geographique.liste_finess_geographique,
    agence_bio.liste_id_bio,
    convention_collective.liste_idcc_etablissement as liste_idcc,
    rge.liste_rge,
    uai.liste_uai,
    etablissement.nom_commercial,
    etablissement.numero_voie,
    etablissement.dernier_numero_voie,
    etablissement.siren,
    etablissement.siret,
    etablissement.statut_diffusion_etablissement as statut_diffusion,
    etablissement.tranche_effectif_salarie,
    etablissement.annee_tranche_effectif_salarie,
    etablissement.date_mise_a_jour_insee,
    etablissement.type_voie
FROM etablissement
    LEFT JOIN finess_geographique ON finess_geographique.siret = etablissement.siret
    LEFT JOIN agence_bio ON agence_bio.siret = etablissement.siret
    LEFT JOIN convention_collective ON convention_collective.siret = etablissement.siret
    LEFT JOIN rge ON rge.siret = etablissement.siret
    LEFT JOIN uai ON uai.siret = etablissement.siret;
"""
