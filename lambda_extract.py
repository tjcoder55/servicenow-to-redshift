import os
import config
from servicenow_client import fetch_all_records, get_max_updated_on
from s3_uploader import upload_to_s3


def lambda_handler(event, context):
    """
    Entry point for the intern-servicenow-extract Lambda.

    Expected event payload from Airflow:
        { "since": "2026-07-06 00:00:00" }   # incremental run
        {}                                     # first ever run — pulls full table

    Returns to Airflow:
        {
            "s3_path":      "s3://bucket/servicenow/incident/latest.csv",
            "new_watermark": "2026-07-07 14:32:00",
            "record_count":  42
        }
    """
    since = event.get("since")
    table = os.environ.get("SN_TABLE", config.SN_TABLE)

    records = fetch_all_records(table=table, since=since)

    if not records:
        print("No new or updated records found.")
        return {
            "s3_path":       None,
            "new_watermark": since,
            "record_count":  0,
        }

    s3_path      = upload_to_s3(records, table)
    new_watermark = get_max_updated_on(records)

    print(f"New high-water mark: {new_watermark}")

    return {
        "s3_path":       s3_path,
        "new_watermark": new_watermark,
        "record_count":  len(records),
        "columns":       list(records[0].keys()),
    }
