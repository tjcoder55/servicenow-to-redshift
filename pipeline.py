import config
from servicenow_client import fetch_all_records
from s3_uploader import upload_to_s3
from redshift_loader import load_from_s3


def run(table: str = None):
    table = table or config.SN_TABLE
    print(f"--- Pipeline starting for table: {table} ---")

    records = fetch_all_records(table)
    if not records:
        print("No records returned from ServiceNow. Exiting.")
        return

    columns = list(records[0].keys())
    s3_path = upload_to_s3(records, table)
    load_from_s3(s3_path, columns, table)

    print(f"--- Pipeline complete for table: {table} ---")


if __name__ == "__main__":
    run()
