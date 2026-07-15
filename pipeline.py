import config
from servicenow_client import fetch_all_records
from s3_uploader import upload_to_s3
from redshift_loader import upsert_from_s3


def run(table: str = None):
    table = table or config.SN_TABLE
    print(f"--- Pipeline starting for table: {table} ---")

    records = fetch_all_records(table)
    if not records:
        print("No records returned from ServiceNow. Exiting.")
        return

    columns = list(records[0].keys())
    s3_path = upload_to_s3(records, table)
    upsert_from_s3(
        s3_path=s3_path,
        columns=columns,
        table=table,
        redshift_conn={
            "host":     config.REDSHIFT_HOST,
            "port":     config.REDSHIFT_PORT,
            "dbname":   config.REDSHIFT_DB,
            "user":     config.REDSHIFT_USER,
            "password": config.REDSHIFT_PASSWORD,
        },
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        schema=config.REDSHIFT_SCHEMA,
    )

    print(f"--- Pipeline complete for table: {table} ---")


if __name__ == "__main__":
    run()
