"""Batch path: roll live readings up into the warehouse, then run the data-quality checks.

Uses the Airflow connection `factorysentinel_db`, set in docker-compose.yml through
AIRFLOW_CONN_FACTORYSENTINEL_DB. The SQL files are the same ones `make` uses by hand.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG

SQL_DIR = Path("/opt/airflow/warehouse")
STORE_RESULTS = """
    INSERT INTO dq_results (check_name, failed_rows, passed, detail)
    SELECT check_name, failed_rows, failed_rows = 0, detail FROM v_quality_checks"""
# The task fails, and Airflow shows it red, when any check other than staleness has failed rows.
FAIL_IF_BROKEN = """
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM v_quality_checks
                   WHERE failed_rows > 0 AND check_name <> 'stream_is_stale') THEN
            RAISE EXCEPTION 'data-quality checks failed, see dq_results';
        END IF;
    END $$"""

with DAG(
    dag_id="warehouse_load",
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["factorysentinel"],
) as dag:
    load_fact = SQLExecuteQueryOperator(
        task_id="load_fact_from_stream",
        conn_id="factorysentinel_db",
        sql=(SQL_DIR / "load" / "03_from_stream.sql").read_text(),
    )
    store_quality = SQLExecuteQueryOperator(
        task_id="store_quality_results", conn_id="factorysentinel_db", sql=STORE_RESULTS
    )
    quality_gate = SQLExecuteQueryOperator(
        task_id="quality_gate", conn_id="factorysentinel_db", sql=FAIL_IF_BROKEN
    )
    load_fact >> store_quality >> quality_gate
