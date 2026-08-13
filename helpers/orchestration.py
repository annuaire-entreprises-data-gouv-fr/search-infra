import logging

from airflow.sdk import Param, get_current_context

logger = logging.getLogger(__name__)

FORCE_REBUILD = "force_rebuild"


def force_rebuild_params(description: str | None = None) -> dict:
    """
    Ajoute le paramètre force_rebuild au DAG param.
    Ce paramètre est ensuite utilisé par force_rebuild_requested() lorsqu'on veut
    bypass des short-circuit.
    Pour ajouter d'autres paramètres à un DAG utiliser le format suivant :
    `params={...} | force_rebuild_params(),`

    Args:
        description(str, None); Optionnel. Description du bouton force_rebuild sur l'UI Airflow.
    """
    return {
        FORCE_REBUILD: Param(
            False,
            type="boolean",
            description=description
            or "Force la reconstruction de zéro des tâches incrémentales.",
        )
    }


def force_rebuild_requested(context: str = "") -> bool:
    """
    Retourne vrai lorsqu'un DAG est lancé avec le paramètre `force_rebuild` à vrai.
    Utile pour bypass des tasks short-circuit ou reconstruire des fichiers incrémentaux de zéro.

    Lit les params du DAG et non la conf du dag run. La conf ne contient que ce qui
    a été soumis au déclenchement, elle ignore la valeur par défaut du Param.

    Args:
        context(str); Optionnel. Précise dans les logs quel traitement est concerné.
    """
    params = get_current_context()["params"]
    if params.get(FORCE_REBUILD):
        suffix = f" for {context}" if context else ""
        logger.info(f"force_rebuild=True{suffix}.")
        return True
    return False
