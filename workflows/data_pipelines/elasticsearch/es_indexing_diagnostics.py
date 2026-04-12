"""
Log which Elasticsearch *server* and *Python client* the indexing DAG is using.

The cluster version comes from the Elasticsearch HTTP API (``GET /`` via ``client.info()``),
so it reflects the real backend (7 vs 9), not only docker-compose or requirements.txt.
"""

from __future__ import annotations

import logging
from typing import Any

import elasticsearch


def _as_mapping(resp: Any) -> dict:
    if resp is None:
        return {}
    if isinstance(resp, dict):
        return resp
    if hasattr(resp, "body") and resp.body is not None:
        return resp.body if isinstance(resp.body, dict) else dict(resp.body)
    return dict(resp)


def log_elasticsearch_versions_for_indexing(
    client: Any, *, log_prefix: str = "indexing"
) -> None:
    """
    Emit one log line with cluster ``version.number`` and one with ``elasticsearch-py`` version.

    Grep Airflow task logs for ``ELASTIC_CLUSTER_VERSION`` to confirm which ES major is targeted.
    """
    client_ver = getattr(elasticsearch, "__versionstr__", None) or getattr(
        elasticsearch, "__version__", "unknown"
    )

    try:
        info = client.info()
        body = _as_mapping(info)
        version = body.get("version") or {}
        if not isinstance(version, dict):
            version = dict(version)
        cluster_number = version.get("number", "unknown")
        build_flavor = version.get("build_flavor") or version.get("distribution", "")
        cluster_name = body.get("cluster_name", "")
    except Exception as e:
        logging.warning(
            "[%s] ELASTIC_CLUSTER_VERSION could not be read: %s "
            "(elasticsearch-py package=%s)",
            log_prefix,
            e,
            client_ver,
        )
        return

    logging.info(
        "[%s] ELASTIC_CLUSTER_VERSION cluster_name=%r number=%r build_flavor=%r "
        "(this is the server; major 7 vs 9 is in number)",
        log_prefix,
        cluster_name,
        cluster_number,
        build_flavor,
    )
    logging.info(
        "[%s] ELASTIC_PYTHON_CLIENT elasticsearch-py=%r (pip package; should be 9.x for ES 9)",
        log_prefix,
        client_ver,
    )
