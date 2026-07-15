import csv
import io
import boto3
import config


def upload_to_s3(records: list, table: str = None) -> str:
    table   = table or config.SN_TABLE
    s3_key  = f"{config.S3_KEY_PREFIX}/{table}/latest.csv"

    if not records:
        raise ValueError("No records to upload — aborting S3 upload.")

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(records[0].keys()))
    writer.writeheader()
    writer.writerows(records)

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=config.S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    s3_path = f"s3://{config.S3_BUCKET}/{s3_key}"
    print(f"Uploaded {len(records)} records to {s3_path}")
    return s3_path
