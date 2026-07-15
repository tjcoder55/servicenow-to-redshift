import json
import sys
import os
from datetime import datetime

import boto3
import psycopg2
from botocore.config import Config
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable

sys.path.insert(0, os.path.dirname(__file__))
from redshift_loader import upsert_from_s3


def _get_redshift_conn() -> dict:
    conn = BaseHook.get_connection("redshift_default")
    return {
        "host":     conn.host,
        "port":     conn.port or 5439,
        "dbname":   conn.schema,
        "user":     conn.login,
        "password": conn.password,
    }


def _get_aws_creds() -> tuple:
    return (
        Variable.get("aws_access_key_id"),
        Variable.get("aws_secret_access_key"),
    )


def _redshift_query(sql: str):
    """Run a single SQL query on Redshift and return all rows."""
    params = _get_redshift_conn()
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


@dag(
    dag_id="servicenow_to_redshift",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["servicenow", "redshift"],
)
def servicenow_pipeline():

    @task
    def get_last_run() -> str | None:
        """Read the high-water mark from Airflow Variable. None on first run = full load."""
        since = Variable.get("sn_incident_last_run", default_var=None)
        print(f"Last run timestamp: {since or 'None (full load)'}")
        return since

    @task
    def get_row_count_before() -> int:
        """Capture row count in Redshift before the load for sanity check after."""
        table = Variable.get("sn_table", default_var="incident")
        try:
            rows = _redshift_query(f"SELECT COUNT(*) FROM public.{table}")
            count = rows[0][0]
        except Exception:
            count = 0
        print(f"Row count before load: {count}")
        return count

    @task
    def invoke_lambda(since: str | None) -> dict:
        """Invoke intern-sn-incident-extract Lambda with the since timestamp."""
        aws_key, aws_secret = _get_aws_creds()
        aws_region = Variable.get("aws_region", default_var="us-west-2")

        client = boto3.client(
            "lambda",
            region_name=aws_region,
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
            config=Config(read_timeout=900, connect_timeout=60, retries={"max_attempts": 0}),
        )
        payload = {"since": since} if since else {}
        response = client.invoke(
            FunctionName="intern-sn-incident-extract",
            InvocationType="RequestResponse",
            Payload=json.dumps(payload),
        )
        result = json.loads(response["Payload"].read())
        print(f"Lambda returned: record_count={result.get('record_count')}, "
              f"new_watermark={result.get('new_watermark')}")
        return result

    @task
    def load_to_redshift(lambda_result: dict):
        """Upsert records from S3 into Redshift."""
        if lambda_result["record_count"] == 0:
            print("No new records — skipping upsert.")
            return
        aws_key, aws_secret = _get_aws_creds()
        upsert_from_s3(
            s3_path=lambda_result["s3_path"],
            columns=lambda_result["columns"],
            table=Variable.get("sn_table", default_var="incident"),
            redshift_conn=_get_redshift_conn(),
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret,
        )

    @task
    def quality_checks(lambda_result: dict, row_count_before: int):
        """
        Data quality checks after load:
          1. sys_id is non-null in Redshift
          2. sys_id is unique in Redshift (no duplicates)
          3. Row count after load >= row count before load
        Raises an exception if any check fails — marks the task red in Airflow.
        """
        if lambda_result["record_count"] == 0:
            print("No records loaded — skipping quality checks.")
            return

        table = Variable.get("sn_table", default_var="incident")

        # Check 1: sys_id non-null
        null_count = _redshift_query(
            f"SELECT COUNT(*) FROM public.{table} WHERE sys_id IS NULL"
        )[0][0]
        if null_count > 0:
            raise ValueError(f"Quality check failed: {null_count} rows have NULL sys_id")
        print("✓ sys_id non-null check passed")

        # Check 2: sys_id uniqueness
        duplicate_count = _redshift_query(
            f"SELECT COUNT(*) - COUNT(DISTINCT sys_id) FROM public.{table}"
        )[0][0]
        if duplicate_count > 0:
            raise ValueError(f"Quality check failed: {duplicate_count} duplicate sys_id values found")
        print("✓ sys_id uniqueness check passed")

        # Check 3: row count sanity
        row_count_after = _redshift_query(
            f"SELECT COUNT(*) FROM public.{table}"
        )[0][0]
        if row_count_after < row_count_before:
            raise ValueError(
                f"Quality check failed: row count dropped from {row_count_before} to {row_count_after}"
            )
        print(f"✓ Row count check passed ({row_count_before} → {row_count_after})")

    @task
    def update_watermark(lambda_result: dict):
        """Save the new high-water mark so the next run starts from here."""
        new_watermark = lambda_result.get("new_watermark")
        if new_watermark:
            Variable.set("sn_incident_last_run", new_watermark)
            print(f"Watermark updated to: {new_watermark}")

    since         = get_last_run()
    count_before  = get_row_count_before()
    lambda_result = invoke_lambda(since)
    load          = load_to_redshift(lambda_result)
    checks        = quality_checks(lambda_result, count_before)
    watermark     = update_watermark(lambda_result)

    load >> checks >> watermark


servicenow_pipeline()
