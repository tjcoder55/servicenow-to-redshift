import psycopg2
import config


def _connect():
    return psycopg2.connect(
        host=config.REDSHIFT_HOST,
        port=config.REDSHIFT_PORT,
        dbname=config.REDSHIFT_DB,
        user=config.REDSHIFT_USER,
        password=config.REDSHIFT_PASSWORD,
    )


def _create_table_if_not_exists(cursor, table: str, columns: list):
    col_defs = ",\n  ".join(f'"{col}" VARCHAR(65535)' for col in columns)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {config.REDSHIFT_SCHEMA}.{table} (
          {col_defs}
        )
    """)


def load_from_s3(s3_path: str, columns: list, table: str = None):
    table = table or config.SN_TABLE

    conn = _connect()
    try:
        with conn.cursor() as cur:
            _create_table_if_not_exists(cur, table, columns)
            cur.execute(f"TRUNCATE TABLE {config.REDSHIFT_SCHEMA}.{table}")

            cur.execute(f"""
                COPY {config.REDSHIFT_SCHEMA}.{table}
                FROM '{s3_path}'
                CREDENTIALS 'aws_access_key_id={config.AWS_ACCESS_KEY_ID};aws_secret_access_key={config.AWS_SECRET_ACCESS_KEY}'
                CSV
                IGNOREHEADER 1
                EMPTYASNULL
                BLANKSASNULL
                TIMEFORMAT 'auto'
            """)

        conn.commit()
        print(f"Loaded data into {config.REDSHIFT_SCHEMA}.{table}")
    finally:
        conn.close()
