import psycopg2


def _connect(host, port, dbname, user, password):
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def _create_table_if_not_exists(cursor, schema, table, columns):
    col_defs = ",\n  ".join(f'"{col}" VARCHAR(65535)' for col in columns)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
          {col_defs}
        )
    """)


def upsert_from_s3(
    s3_path: str,
    columns: list,
    table: str,
    redshift_conn: dict,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    schema: str = "public",
):
    """
    Idempotent upsert from S3 into Redshift:
      1. COPY S3 data into a temp staging table
      2. DELETE rows from target where sys_id matches staging
      3. INSERT all rows from staging into target
    Running this twice with the same S3 file produces identical results.
    """
    staging = f"{table}_staging"
    creds   = f"aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}"

    conn = _connect(**redshift_conn)
    try:
        with conn.cursor() as cur:
            _create_table_if_not_exists(cur, schema, table, columns)

            cur.execute(f"DROP TABLE IF EXISTS {schema}.{staging}")
            cur.execute(f"CREATE TABLE {schema}.{staging} (LIKE {schema}.{table})")

            cur.execute(f"""
                COPY {schema}.{staging}
                FROM '{s3_path}'
                CREDENTIALS '{creds}'
                CSV
                IGNOREHEADER 1
                EMPTYASNULL
                BLANKSASNULL
                TIMEFORMAT 'auto'
            """)

            cur.execute(f"""
                DELETE FROM {schema}.{table}
                WHERE sys_id IN (SELECT sys_id FROM {schema}.{staging})
            """)

            cur.execute(f"""
                INSERT INTO {schema}.{table}
                SELECT * FROM {schema}.{staging}
            """)

            cur.execute(f"DROP TABLE IF EXISTS {schema}.{staging}")

        conn.commit()
        print(f"Upserted data into {schema}.{table}")
    finally:
        conn.close()
