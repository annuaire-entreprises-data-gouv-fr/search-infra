# Email templates

`email_notification.html` is the body sent by `EmailNotification` in `helpers/notification.py`.
It is used on every DAG in the `on_failure_callback` setting.

## Variables

| Variable                                           | Source |
| -------------------------------------------------- | ----------------------------------------------------------- |
| `env`                                              | `AIRFLOW_ENV` dans `config.py` |
| `tz`                                               | `EMAIL_TIMEZONE` (`Europe/Paris`) dans `notification.py` |
| `dag.dag_id`                                       | the failing DAG name |
| `reason`                                           | the failure message |
| `dag_run.state`, `.run_id`, `.run_type`            | DAG information |
| `dag_run.logical_date`, `.start_date`, `.end_date` | datetimes of DAG run |
| `ti.log_url`                                       | link to the task log |
| `ti.task_id`, `ti.try_number`, `.max_tries`        | task information |

## Warning

- The DAG Jinja env uses `StrictUndefined`, so calling a variable not set will raise an error when
  sending the email time. `x is defined` checks are required.
- `SmtpNotifier._read_template` removes every `\n` from the template before rendering.
  So we need to keep each Jinja expression in a single line and use `<!-- prettier-ignore -->`
  to avoid formatters adding new lines.
