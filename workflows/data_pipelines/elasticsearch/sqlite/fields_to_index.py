select_fields_to_index_query = """SELECT
            ul.activite_principale_unite_legale as activite_principale_unite_legale,
            ul.caractere_employeur as caractere_employeur,
            ul.categorie_entreprise as categorie_entreprise,
            ul.date_creation_unite_legale as date_creation_unite_legale,
            ul.date_fermeture_unite_legale as date_fermeture,
            ul.date_mise_a_jour_insee as date_mise_a_jour_insee,
            ul.date_mise_a_jour_rne as date_mise_a_jour_rne,
            ul.denomination_usuelle_1 as denomination_usuelle_1_unite_legale,
            ul.denomination_usuelle_2 as denomination_usuelle_2_unite_legale,
            ul.denomination_usuelle_3 as denomination_usuelle_3_unite_legale,
            ul.economie_sociale_solidaire_unite_legale as
            economie_sociale_solidaire_unite_legale,
            ul.etat_administratif_unite_legale as etat_administratif_unite_legale,
            ul.from_insee as from_insee,
            ul.from_rne as from_rne,
            ul.identifiant_association_unite_legale as
            identifiant_association_unite_legale,
            ul.nature_juridique_unite_legale as nature_juridique_unite_legale,
            ul.nom as nom,
            ul.nom_raison_sociale as nom_raison_sociale,
            ul.nom_usage as nom_usage,
            ul.prenom as prenom,
            ul.sigle as sigle,
            ul.siren,
            st.siret as siret_siege,
            ul.tranche_effectif_salarie_unite_legale as
            tranche_effectif_salarie_unite_legale,
            ul.statut_diffusion_unite_legale as
            statut_diffusion_unite_legale,
            ul.est_societe_mission as est_societe_mission,
            ul.annee_categorie_entreprise as annee_categorie_entreprise,
            ul.annee_tranche_effectif_salarie as annee_tranche_effectif_salarie,
            (SELECT sirets_par_idcc FROM convention_collective WHERE
                        siren = ul.siren) as sirets_par_idcc,
            (SELECT liste_idcc_unite_legale FROM convention_collective WHERE
                        siren = ul.siren) as liste_idcc_unite_legale,
            (SELECT count FROM count_etablissement ce WHERE ce.siren = ul.siren) as
            nombre_etablissements,
            (SELECT count FROM count_etablissement_ouvert ceo WHERE ceo.siren = ul.siren) as
            nombre_etablissements_ouverts,
            (
                SELECT json_object(
                    'ca', ca,
                    'resultat_net', resultat_net,
                    'date_cloture_exercice', date_cloture_exercice,
                    'annee_cloture_exercice', annee_cloture_exercice
                )
                FROM
                (
                    SELECT ca, resultat_net,
                    date_cloture_exercice, annee_cloture_exercice
                    FROM bilan_financier
                    WHERE siren = ul.siren
                )
            ) as bilan_financier,
            (SELECT json_group_array(
                json_object(
                    'siren', siren,
                    'date_mise_a_jour', date_mise_a_jour,
                    'date_de_naissance', date_de_naissance,
                    'nom', nom,
                    'nom_usage', nom_usage,
                    'prenoms', prenoms,
                    'nationalite', nationalite,
                    'role_description', role_description
                    )
                ) FROM
                (
                    SELECT siren, date_mise_a_jour, date_de_naissance, nom,
                    nom_usage, prenoms, nationalite, role_description
                    FROM dirigeant_pp
                    WHERE siren = ul.siren
                )
            ) as dirigeants_pp,
            (SELECT json_group_array(
                    json_object(
                        'siren', siren,
                        'date_mise_a_jour', date_mise_a_jour,
                        'denomination', denomination,
                        'siren_dirigeant', siren_dirigeant,
                        'role_description', role_description,
                        'forme_juridique', forme_juridique
                        )
                    ) FROM
                    (
                        SELECT siren, date_mise_a_jour, denomination, siren_dirigeant,
                        role_description, forme_juridique
                        FROM dirigeant_pm
                        WHERE siren = ul.siren
                    )
                ) as dirigeants_pm,
            (SELECT json_group_array(
                    json_object(
                        'activite_principale',activite_principale,
                        'activite_principale_registre_metier',
                        activite_principale_registre_metier,
                        'ancien_siege',ancien_siege,
                        'caractere_employeur',caractere_employeur,
                        'cedex',cedex,
                        'code_pays_etranger',code_pays_etranger,
                        'code_postal',code_postal,
                        'commune',commune,
                        'complement_adresse',complement_adresse,
                        'date_creation',date_creation,
                        'date_debut_activite',date_debut_activite,
                        'date_fermeture',date_fermeture,
                        'distribution_speciale',distribution_speciale,
                        'enseigne_1',enseigne_1,
                        'enseigne_2',enseigne_2,
                        'enseigne_3',enseigne_3,
                        'est_siege',est_siege,
                        'etat_administratif',etat_administratif_etablissement,
                        'geo_adresse',geo_adresse,
                        'geo_id',geo_id,
                        'geo_score',geo_score,
                        'indice_repetition',indice_repetition,
                        'latitude',latitude,
                        'libelle_cedex',libelle_cedex,
                        'libelle_commune',libelle_commune,
                        'libelle_commune_etranger',libelle_commune_etranger,
                        'libelle_pays_etranger',libelle_pays_etranger,
                        'libelle_voie',libelle_voie,
                        'liste_finess',liste_finess,
                        'liste_id_bio',liste_id_bio,
                        'liste_idcc',liste_idcc,
                        'liste_rge',liste_rge,
                        'liste_uai',liste_uai,
                        'longitude',longitude,
                        'nom_commercial',nom_commercial,
                        'numero_voie',numero_voie,
                        'siren',siren,
                        'siret',siret,
                        'statut_diffusion_etablissement',
                        statut_diffusion_etablissement,
                        'tranche_effectif_salarie',tranche_effectif_salarie,
                        'annee_tranche_effectif_salarie',annee_tranche_effectif_salarie,
                        'date_mise_a_jour_insee',date_mise_a_jour_insee,
                        'type_voie',type_voie,
                        'x',x,
                        'y',y
                        )
                    ) FROM
                    (
                        SELECT
                        s.activite_principale as activite_principale,
                        s.activite_principale_registre_metier as
                        activite_principale_registre_metier,
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
                        s.etat_administratif_etablissement as
                        etat_administratif_etablissement,
                        s.geo_adresse as geo_adresse,
                        s.geo_id as geo_id,
                        s.geo_score as geo_score,
                        s.indice_repetition as indice_repetition,
                        s.latitude as latitude,
                        s.libelle_cedex as libelle_cedex,
                        s.libelle_commune as libelle_commune,
                        s.libelle_commune_etranger as libelle_commune_etranger,
                        s.libelle_pays_etranger as libelle_pays_etranger,
                        s.libelle_voie as libelle_voie,
                        s.longitude as longitude,
                        (SELECT liste_finess FROM finess WHERE siret = s.siret) as
                        liste_finess,
                        (SELECT liste_id_bio FROM agence_bio WHERE siret = s.siret) as
                        liste_id_bio,
                        (SELECT liste_idcc_etablissement FROM convention_collective
                        WHERE siret = s.siret) as liste_idcc,
                        (SELECT liste_rge FROM rge WHERE siret = s.siret) as liste_rge,
                        (SELECT liste_uai FROM uai WHERE siret = s.siret) as liste_uai,
                        s.nom_commercial as nom_commercial,
                        s.numero_voie as numero_voie,
                        s.siren as siren,
                        s.siret as siret,
                        s.statut_diffusion_etablissement as
                        statut_diffusion_etablissement,
                        s.tranche_effectif_salarie as
                        tranche_effectif_salarie,
                        s.annee_tranche_effectif_salarie as
                        annee_tranche_effectif_salarie,
                        s.date_mise_a_jour_insee as date_mise_a_jour_insee,
                        s.type_voie as type_voie,
                        s.x as x,
                        s.y as y
                        FROM etablissement s
                        WHERE s.siren = ul.siren
                    )
                ) as etablissements,
            (SELECT json_object(
                        'activite_principale',activite_principale,
                        'activite_principale_registre_metier',
                        activite_principale_registre_metier,
                        'caractere_employeur',caractere_employeur,
                        'cedex',cedex,
                        'code_pays_etranger',code_pays_etranger,
                        'code_postal',code_postal,
                        'commune',commune,
                        'complement_adresse',complement_adresse,
                        'date_creation',date_creation,
                        'date_debut_activite',date_debut_activite,
                        'date_fermeture',date_fermeture,
                        'distribution_speciale',distribution_speciale,
                        'enseigne_1',enseigne_1,
                        'enseigne_2',enseigne_2,
                        'enseigne_3',enseigne_3,
                        'est_siege',est_siege,
                        'etat_administratif',etat_administratif_etablissement,
                        'from_insee',from_insee,
                        'from_rne',from_rne,
                        'geo_adresse',geo_adresse,
                        'geo_id',geo_id,
                        'geo_score',geo_score,
                        'indice_repetition',indice_repetition,
                        'latitude',latitude,
                        'libelle_cedex',libelle_cedex,
                        'libelle_commune',libelle_commune,
                        'libelle_commune_etranger',libelle_commune_etranger,
                        'libelle_pays_etranger',libelle_pays_etranger,
                        'libelle_voie',libelle_voie,
                        'liste_finess',liste_finess,
                        'liste_id_bio',liste_id_bio,
                        'liste_idcc',liste_idcc,
                        'liste_rge',liste_rge,
                        'liste_uai',liste_uai,
                        'longitude',longitude,
                        'nom_commercial',nom_commercial,
                        'numero_voie',numero_voie,
                        'siren',siren,
                        'siret',siret,
                        'statut_diffusion_etablissement',
                        statut_diffusion_etablissement,
                        'tranche_effectif_salarie',tranche_effectif_salarie,
                        'annee_tranche_effectif_salarie',
                        annee_tranche_effectif_salarie,
                        'type_voie',type_voie,
                        'date_mise_a_jour_insee',date_mise_a_jour_insee,
                        'date_mise_a_jour_rne',date_mise_a_jour_rne,
                        'x',x,
                        'y',y
                        )
                    FROM
                    (
                        SELECT
                        s.activite_principale as activite_principale,
                        s.activite_principale_registre_metier as
                        activite_principale_registre_metier,
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
                        s.etat_administratif_etablissement as
                        etat_administratif_etablissement,
                        s.geo_adresse as geo_adresse,
                        s.geo_id as geo_id,
                        s.geo_score as geo_score,
                        s.indice_repetition as indice_repetition,
                        s.latitude as latitude,
                        s.libelle_cedex as libelle_cedex,
                        s.libelle_commune as libelle_commune,
                        s.libelle_commune_etranger as libelle_commune_etranger,
                        s.libelle_pays_etranger as libelle_pays_etranger,
                        s.libelle_voie as libelle_voie,
                        (SELECT liste_finess FROM finess WHERE siret = s.siret) as
                        liste_finess,
                        (SELECT liste_id_bio FROM agence_bio WHERE siret = s.siret) as
                        liste_id_bio,
                        (SELECT liste_idcc_etablissement FROM convention_collective
                        WHERE siret = s.siret) as liste_idcc,
                        (SELECT liste_rge FROM rge WHERE siret = s.siret) as liste_rge,
                        (SELECT liste_uai FROM uai WHERE siret = s.siret) as liste_uai,
                        s.longitude as longitude,
                        s.nom_commercial as nom_commercial,
                        s.numero_voie as numero_voie,
                        s.siren as siren,
                        s.siret as siret,
                        s.statut_diffusion_etablissement as
                        statut_diffusion_etablissement,
                        s.tranche_effectif_salarie as tranche_effectif_salarie,
                        s.annee_tranche_effectif_salarie as
                        annee_tranche_effectif_salarie,
                        s.type_voie as type_voie,
                        s.date_mise_a_jour_insee as date_mise_a_jour_insee,
                        s.date_mise_a_jour_rne as date_mise_a_jour_rne,
                        s.x as x,
                        s.y as y
                        FROM siege as s
                        WHERE s.siren = st.siren
                    )
                ) as siege,
            (SELECT est_entrepreneur_spectacle FROM spectacle WHERE siren = ul.siren) as
             est_entrepreneur_spectacle,
            (SELECT statut_entrepreneur_spectacle FROM spectacle WHERE siren = ul.siren)
              as statut_entrepreneur_spectacle,
            (SELECT egapro_renseignee FROM egapro WHERE siren = ul.siren) as
             egapro_renseignee,
            (SELECT bilan_ges_renseigne FROM bilan_ges WHERE siren = ul.siren) as
             bilan_ges_renseigne,
            (SELECT est_achats_responsables FROM achats_responsables WHERE siren = ul.siren) as
             est_achats_responsables,
            (SELECT est_alim_confiance FROM alim_confiance WHERE siren = ul.siren) as
             est_alim_confiance,
            (SELECT est_patrimoine_vivant FROM patrimoine_vivant WHERE siren = ul.siren) as
             est_patrimoine_vivant,
            (SELECT DISTINCT colter_code_insee FROM colter WHERE siren = ul.siren) as
            colter_code_insee,
            (SELECT DISTINCT colter_code FROM colter WHERE siren = ul.siren) as colter_code,
            (SELECT DISTINCT colter_niveau FROM colter WHERE siren = ul.siren) as colter_niveau,
            (SELECT est_ess_france FROM ess_france WHERE siren = ul.siren) as
            est_ess_france,
            (SELECT json_group_array(
                json_object(
                    'siren', siren,
                    'nom', nom,
                    'prenom', prenom,
                    'date_naissance', date_naissance,
                    'sexe', sexe,
                    'fonction', fonction
                    )
                ) FROM
                (
                    SELECT DISTINCT siren, nom, prenom, date_naissance,
                    sexe, fonction
                    FROM elus
                    WHERE siren = ul.siren
                )
            ) as colter_elus,
            (SELECT est_qualiopi FROM organisme_formation WHERE siren = ul.siren) as
            est_qualiopi,
            (SELECT liste_id_organisme_formation FROM organisme_formation
            WHERE siren = ul.siren)  as liste_id_organisme_formation,
            (SELECT est_siae  FROM marche_inclusion WHERE siren = ul.siren) AS est_siae,
            (SELECT type_siae FROM marche_inclusion WHERE siren = ul.siren)
            AS type_siae,
            (
                SELECT json_object(
                    'date_immatriculation', date_immatriculation,
                    'date_radiation', date_radiation,
                    'indicateur_associe_unique', indicateur_associe_unique,
                    'capital_social', capital_social,
                    'date_cloture_exercice', date_cloture_exercice,
                    'duree_personne_morale', duree_personne_morale,
                    'nature_entreprise', nature_entreprise,
                    'date_debut_activite', date_debut_activite,
                    'capital_variable', capital_variable,
                    'devise_capital', devise_capital
                )
                FROM
                (
                    SELECT date_immatriculation, date_radiation,
                    indicateur_associe_unique, capital_social,
                    date_cloture_exercice, duree_personne_morale, nature_entreprise,
                    date_debut_activite, capital_variable, devise_capital
                    FROM immatriculation
                    WHERE siren = ul.siren
                )
            ) as immatriculation
            FROM
                unite_legale ul
            LEFT JOIN
                siege st
            ON
                ul.siren = st.siren
            WHERE ul.siren IS NOT NULL;"""
