import os
from dotenv import load_dotenv

load_dotenv()

# ServiceNow
SN_BASE_URL = os.getenv("SN_BASE_URL", "https://stanforddev.service-now.com")
SN_USERNAME = os.getenv("SN_USERNAME")
SN_PASSWORD = os.getenv("SN_PASSWORD")
SN_TABLE    = os.getenv("SN_TABLE", "incident")

# S3
S3_BUCKET    = os.getenv("S3_BUCKET")
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "servicenow")
AWS_REGION   = os.getenv("AWS_REGION", "us-west-2")
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Redshift
REDSHIFT_HOST     = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT     = int(os.getenv("REDSHIFT_PORT", "5439"))
REDSHIFT_DB       = os.getenv("REDSHIFT_DB")
REDSHIFT_USER     = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_SCHEMA   = os.getenv("REDSHIFT_SCHEMA", "public")
