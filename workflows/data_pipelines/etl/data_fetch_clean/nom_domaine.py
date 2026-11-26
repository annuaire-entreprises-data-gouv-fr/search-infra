from collections import defaultdict
from collections.abc import Iterable

from data_pipelines_annuaire.helpers.utils import normalize_nom_domaine

# Environ ~8 % des noms de domaine du RNE sont déclarés par de nombreuses entreprises sans lien
# comme inpi.fr (23k Siren), gmail.com, amazon.fr, uber.fr ou iadfrance.fr.
# À partir de 5 Siren ce sont presque uniquement des plateformes ou des mots génériques
# comme martin.fr ou bijoux.fr donc on les écarte, même si cela implique quelques faux négatifs.
MAX_SIREN_PER_NOM_DOMAINE = 5


def clean_noms_de_domaine(
    rows: Iterable[tuple[str, str | None]],
) -> list[tuple[str, str | None, str | None]]:
    """
    Ajoute le nom de domaine normalisé à chaque tuple (siren, nom_domaine_original).
    Il vaut None quand la valeur originale n'est pas un nom de domaine valide ou est partagée par trop de Siren.
    Le formulaire du guichet unique ne valide pas le champ.
    On garde toutes les lignes pour pouvoir comparer les deux valeurs dans la base.
    """
    rows_normalized = [
        (siren, nom_domaine_original, normalize_nom_domaine(nom_domaine_original))
        for siren, nom_domaine_original in rows
    ]

    sirens_by_nom_domaine: dict[str, set[str]] = defaultdict(set)
    for siren, _, nom_domaine in rows_normalized:
        if nom_domaine:
            sirens_by_nom_domaine[nom_domaine].add(siren)

    return [
        (
            siren,
            nom_domaine_original,
            nom_domaine
            if nom_domaine
            and len(sirens_by_nom_domaine[nom_domaine]) < MAX_SIREN_PER_NOM_DOMAINE
            else None,
        )
        for siren, nom_domaine_original, nom_domaine in rows_normalized
    ]
