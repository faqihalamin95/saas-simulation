"""
saas_platform_pipeline.py
─────────────────────────
Orchestrates the full Y2 SaaS data pipeline:

  ingest_r2_to_snowflake
        ↓
  dbt_run_staging
        ↓
  dbt_test_staging  (soft warning — pipeline continues on warn)
        ↓
  dbt_run_foundation
        ↓
  dbt_test_foundation  (hard stop — pipeline halts on error)
        ↓
  dbt_run_marts

Schedule: Monthly on the 1st at 06:00 UTC
Alerts:   Telegram — failure per task, success per DAG run
"""

import os
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Paths inside the container ────────────────────────────
PROJECT_DIR = "/opt/airflow/project"
DBT_DIR     = f"{PROJECT_DIR}/dbt"
SRC_DIR     = f"{PROJECT_DIR}/src"

# ── dbt base commands ─────────────────────────────────────
# --target-path /tmp/dbt-target: redirect dbt output (manifest,
# partial_parse.msgpack, etc.) to /tmp which is always writable,
# avoiding permission errors on bind-mounted project directories.
DBT_RUN  = f"cd {DBT_DIR} && dbt run  --target-path /tmp/dbt-target"
DBT_TEST = f"cd {DBT_DIR} && dbt test --target-path /tmp/dbt-target"

# ── Telegram helpers ──────────────────────────────────────
def _telegram_post(bot_token, chat_id, message):
    """Send a message to Telegram. Silently fails if API is unreachable."""
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"},
            timeout=10
        )
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

def _get_telegram_creds():
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id   = os.environ.get("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        print("Telegram credentials not set, skipping alert.")
        return None, None
    return bot_token, chat_id


def send_telegram_failure(context):
    """Called on any task failure — includes task name and log link."""
    bot_token, chat_id = _get_telegram_creds()
    if not bot_token:
        return

    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id  = context["run_id"]
    log_url = context["task_instance"].log_url

    message = (
        f"❌ *Pipeline Failed*\n"
        f"DAG: `{dag_id}`\n"
        f"Task: `{task_id}`\n"
        f"Run: `{run_id}`\n"
        f"[View Logs]({log_url})"
    )
    _telegram_post(bot_token, chat_id, message)


def send_telegram_success(context):
    """Called once when the full DAG completes successfully."""
    bot_token, chat_id = _get_telegram_creds()
    if not bot_token:
        return

    dag_id   = context["dag"].dag_id
    run_id   = context["run_id"]
    duration = context["dag_run"].end_date - context["dag_run"].start_date

    message = (
        f"✅ *Pipeline Success*\n"
        f"DAG: `{dag_id}`\n"
        f"Run: `{run_id}`\n"
        f"Duration: `{str(duration).split('.')[0]}`"
    )
    _telegram_post(bot_token, chat_id, message)


# ── Default args ──────────────────────────────────────────
default_args = {
    "owner": "faqih",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": send_telegram_failure,  # per task
}

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id="saas_platform_pipeline",
    description="Y2 SaaS pipeline: R2 → Snowflake → dbt staging → foundation → marts",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 1 * *",  # monthly, 1st at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["saas", "snowflake", "dbt"],
    on_success_callback=send_telegram_success,     # per DAG run
) as dag:

    # ── TASK 1: Ingest R2 → Snowflake RAW ─────────────────
    ingest = BashOperator(
        task_id="ingest_r2_to_snowflake",
        bash_command=f"cd {PROJECT_DIR} && python src/ingestion/ingest_r2.py",
    )

    # ── TASK 2: dbt run staging ────────────────────────────
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_RUN} --select staging",
    )

    # ── TASK 3: dbt test staging (soft — warn only) ────────
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT_TEST} --select staging || true",
        # '|| true' ensures test warnings don't fail the task
    )

    # ── TASK 4: dbt run foundation ─────────────────────────
    dbt_run_foundation = BashOperator(
        task_id="dbt_run_foundation",
        bash_command=f"{DBT_RUN} --select foundation",
    )

    # ── TASK 5: dbt test foundation (hard stop) ────────────
    # If this fails, marts will NOT be built
    dbt_test_foundation = BashOperator(
        task_id="dbt_test_foundation",
        bash_command=f"{DBT_TEST} --select foundation",
    )

    # ── TASK 6: dbt run marts ──────────────────────────────
    # Only runs if foundation tests pass
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_RUN} --select marts",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Dependencies ───────────────────────────────────────
    (
        ingest
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_foundation
        >> dbt_test_foundation
        >> dbt_run_marts
    )