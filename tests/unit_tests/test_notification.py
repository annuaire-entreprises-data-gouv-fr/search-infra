from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from jinja2 import StrictUndefined
from jinja2.sandbox import SandboxedEnvironment

from data_pipelines_annuaire.config import AIRFLOW_ENV
from data_pipelines_annuaire.helpers.notification import (
    EMAIL_TIMEZONE,
    EmailNotification,
)


def render(notifier, context):
    context = {**context, "env": notifier.env, "tz": notifier.tz}
    jinja_env = SandboxedEnvironment(undefined=StrictUndefined, cache_size=0)
    return jinja_env.from_string(notifier.html_content).render(**context)


@pytest.fixture
def notifier():
    """Holds the exact string Airflow renders, newlines stripped by SmtpNotifier."""
    return EmailNotification(to=["ops@example.fr"])


@pytest.fixture
def full_context():
    return {
        "dag": SimpleNamespace(dag_id="data_processing_avocat"),
        "reason": "Task download_data failed",
        "dag_run": SimpleNamespace(
            state="failed",
            run_id="manual__2026-07-08T03:00:00",
            run_type="scheduled",
            logical_date=datetime(2026, 7, 8, 3, 0, tzinfo=UTC),
            start_date=datetime(2026, 7, 8, 3, 0, 1, tzinfo=UTC),
            end_date=datetime(2026, 7, 8, 3, 4, 12, tzinfo=UTC),
        ),
        "ti": SimpleNamespace(
            task_id="download_data",
            try_number=2,
            max_tries=1,
            log_url="https://airflow.example/log",
        ),
    }


def test_template_renders_with_task_instance(notifier, full_context):
    html = render(notifier, full_context)

    assert "data_processing_avocat" in html
    assert "download_data" in html
    assert "2 sur 2" in html
    assert "https://airflow.example/log" in html
    assert "#000091" in html
    assert "Annuaire des Entreprises" in html
    assert f"Environnement&nbsp;: {AIRFLOW_ENV}" in html


def test_template_renders_without_task_instance(notifier, full_context):
    """DAG-level failure callbacks may not include a task instance."""
    context = {k: v for k, v in full_context.items() if k != "ti"}

    html = render(notifier, context)

    assert "data_processing_avocat" in html
    # Task-only sections are omitted, no dangling template errors
    assert "Consulter les logs" not in html
    assert "Tentative" not in html


def test_email_notification_defaults(notifier):
    assert notifier.html_content is not None
    assert "Annuaire des Entreprises" in notifier.html_content
    assert "{{ dag.dag_id }}" in notifier.subject
    assert "{{ env }}" in notifier.subject


def test_env_is_exposed_to_the_template(notifier):
    """Airflow ENV must be a template field."""
    assert notifier.env == AIRFLOW_ENV
    assert "env" in notifier.template_fields


def test_tz_is_exposed_to_the_template(notifier):
    """Europe/Paris must be a template field."""
    assert notifier.tz == EMAIL_TIMEZONE
    assert "tz" in notifier.template_fields


def test_dates_render_in_paris_time(notifier, full_context):
    """Airflow stores UTC, the email shows Europe/Paris (UTC+2 in July)."""
    html = render(notifier, full_context)

    assert "08/07/2026 05:00 CEST" in html
    assert "08/07/2026 05:00:01 CEST" in html
    assert "08/07/2026 05:04:12 CEST" in html


def test_email_notification_subject_override():
    notifier = EmailNotification(to="ops@example.fr", subject="custom subject")

    assert notifier.subject == "custom subject"
    assert notifier.html_content is not None
