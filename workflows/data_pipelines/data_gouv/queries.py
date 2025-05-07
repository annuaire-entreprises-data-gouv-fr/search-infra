ul_fields_to_select = """
SELECT ul.etat_administratif_unite_legale as etat_administratif,
    ul.statut_diffusion_unite_legale as statut_diffusion,
    (
        SELECT colter_code
        FROM colter
        WHERE siren = ul.siren
    ) as colter_code,
    (
        SELECT colter_code_insee
        FROM colter
        WHERE siren = ul.siren
    ) as colter_code_insee,
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
                WHERE siren = ul.siren
            )
    ) as colter_elus,
    (
        SELECT colter_niveau
        FROM colter
        WHERE siren = ul.siren
    ) as colter_niveau,
    ul.date_mise_a_jour_insee as date_mise_a_jour_insee,
    ul.date_mise_a_jour_rne as date_mise_a_jour_rne,
    (
        SELECT egapro_renseignee
        FROM egapro
        WHERE siren = ul.siren
    ) as egapro_renseignee,
    (
        SELECT est_achats_responsables
        FROM achats_responsables
        WHERE siren = ul.siren
    ) as est_achats_responsables,
    (
        SELECT est_alim_confiance
        FROM alim_confiance
        WHERE siren = ul.siren
    ) as est_alim_confiance,
    (
        SELECT est_patrimoine_vivant
        FROM patrimoine_vivant
        WHERE siren = ul.siren
    ) as est_patrimoine_vivant,
    ul.economie_sociale_solidaire_unite_legale as economie_sociale_solidaire,
    (
        SELECT est_entrepreneur_spectacle
        FROM spectacle
        WHERE siren = ul.siren
    ) as est_entrepreneur_spectacle,
    (
        SELECT est_ess_france
        FROM ess_france
        WHERE siren = ul.siren
    ) as est_ess_france,
    (
        SELECT est_qualiopi
        FROM organisme_formation
        WHERE siren = ul.siren
    ) as est_qualiopi,
    (
        SELECT est_siae
        FROM marche_inclusion
        WHERE siren = ul.siren
    ) as est_siae,
    ul.identifiant_association_unite_legale as identifiant_association,
    ul.est_societe_mission as est_societe_mission,
    (
        SELECT liste_id_organisme_formation
        FROM organisme_formation
        WHERE siren = ul.siren
    ) as liste_id_organisme_formation,
    (
        SELECT liste_idcc_unite_legale
        FROM convention_collective
        WHERE siren = ul.siren
    ) as liste_idcc,
    ul.nature_juridique_unite_legale as nature_juridique,
    ul.nom as nom,
    ul.nom_raison_sociale as nom_raison_sociale,
    ul.nom_usage as nom_usage,
    (
        SELECT count
        FROM count_etablissement ce
        WHERE ce.siren = ul.siren
    ) as nombre_etablissements,
    (
        SELECT count
        FROM count_etablissement_ouvert ceo
        WHERE ceo.siren = ul.siren
    ) as nombre_etablissements_ouverts,
    ul.prenom as prenom,
    ul.siren,
    st.siret as siret_siege,
    ul.sigle as sigle,
    (
        SELECT statut_entrepreneur_spectacle
        FROM spectacle
        WHERE siren = ul.siren
    ) as statut_entrepreneur_spectacle,
    (
        SELECT type_siae
        FROM marche_inclusion
        WHERE siren = ul.siren
    ) as type_siae
FROM unite_legale ul
    LEFT JOIN siege st ON ul.siren = st.siren
WHERE ul.siren IS NOT NULL;
"""


etab_fields_to_select = """SELECT s.activite_principale as activite_principale,
    s.activite_principale_registre_metier as activite_principale_registre_metier,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM ancien_siege
            WHERE siret = s.siret
            )
            THEN TRUE
        ELSE FALSE
    END AS ancien_siege,
    s.caractere_employeur as caractere_employeur,
    s.cedex as cedex,
    s.code_pays_etranger as code_pays_etranger,
    s.code_postal as code_postal,
    s.commune as commune,
    s.complement_adresse as complement_adresse,
    s.date_creation as date_creation,
    s.date_debut_activite as date_debut_activite,
    s.date_fermeture_etablissement as date_fermeture,
    s.distribution_speciale as distribution_speciale,
    s.enseigne_1 as enseigne_1,
    s.enseigne_2 as enseigne_2,
    s.enseigne_3 as enseigne_3,
    s.est_siege as est_siege,
    s.etat_administratif_etablissement as etat_administratif,
    s.indice_repetition as indice_repetition,
    s.libelle_cedex as libelle_cedex,
    s.libelle_commune as libelle_commune,
    s.libelle_commune_etranger as libelle_commune_etranger,
    s.libelle_pays_etranger as libelle_pays_etranger,
    s.libelle_voie as libelle_voie,
    (
        SELECT liste_finess
        FROM finess
        WHERE siret = s.siret
    ) as liste_finess,
    (
        SELECT liste_id_bio
        FROM agence_bio
        WHERE siret = s.siret
    ) as liste_id_bio,
    (
        SELECT liste_idcc_etablissement
        FROM convention_collective
        WHERE siret = s.siret
    ) as liste_idcc,
    (
        SELECT liste_rge
        FROM rge
        WHERE siret = s.siret
    ) as liste_rge,
    (
        SELECT liste_uai
        FROM uai
        WHERE siret = s.siret
    ) as liste_uai,
    s.nom_commercial as nom_commercial,
    s.numero_voie as numero_voie,
    s.siren as siren,
    s.siret as siret,
    s.statut_diffusion_etablissement as statut_diffusion,
    s.tranche_effectif_salarie as tranche_effectif_salarie,
    s.annee_tranche_effectif_salarie as annee_tranche_effectif_salarie,
    s.date_mise_a_jour_insee as date_mise_a_jour_insee,
    s.type_voie as type_voie,
    s.x as x,
    s.y as y
FROM etablissement s;
                        """
