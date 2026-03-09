import os
import re
import time
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv

# ------------------------------------------------------------
# 1. Load environment variables from the absolute .env path
# ------------------------------------------------------------
env_path = Path(r"C:\your\base\path\AWS Data Lakehouse\.env")
if not env_path.exists():
    raise FileNotFoundError(f".env file not found at {env_path}")
load_dotenv(dotenv_path=env_path)

# ------------------------------------------------------------
# 2. Required configuration – all must be set in .env
# ------------------------------------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise ValueError(
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in the .env file.\n"
        "Please add them and try again."
    )

BUCKET = os.getenv("BUCKET")
if not BUCKET:
    raise ValueError("BUCKET must be set in the .env file.")

RAW_PREFIX = os.getenv("RAW_PREFIX")
if RAW_PREFIX is None:
    raise ValueError("RAW_PREFIX must be set in the .env file (e.g., 'Training Batch/').")

DATABASE = os.getenv("DATABASE")
if not DATABASE:
    raise ValueError("DATABASE must be set in the .env file.")

RAW_TABLE = os.getenv("RAW_TABLE")
if not RAW_TABLE:
    raise ValueError("RAW_TABLE must be set in the .env file.")

ATHENA_REGION = os.getenv("ATHENA_REGION")
if not ATHENA_REGION:
    raise ValueError("ATHENA_REGION must be set in the .env file.")

ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT")
if not ATHENA_OUTPUT:
    raise ValueError("ATHENA_OUTPUT must be set in the .env file (e.g., 's3://my-bucket/athena-results/').")

# ------------------------------------------------------------
# 3. Create boto3 session with explicit credentials only
# ------------------------------------------------------------
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=ATHENA_REGION
)
s3 = session.client('s3')
athena = session.client('athena')

# ------------------------------------------------------------
# 4. Helper: run Athena query and wait for completion
# ------------------------------------------------------------
def run_athena_query(query, silent=False):
    """Execute a query and wait. If silent=True, suppress the success message."""
    execution = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    query_id = execution['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state != 'SUCCEEDED':
        reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"Athena query failed: {query}\nReason: {reason}")

    if not silent:
        print(f"Query succeeded: {query[:80]}...")
    return query_id

def get_count_from_query(query_id):
    """Fetch the single count value from a SELECT COUNT(*) query result."""
    result = athena.get_query_results(QueryExecutionId=query_id)
    # The count is in the first row, first column (after header)
    return int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])

# ------------------------------------------------------------
# 5. Main execution – idempotent insert using MERGE with DISTINCT source
# ------------------------------------------------------------
try:
    # Create database if not exists
    print("Creating database if not exists...")
    run_athena_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

    # Create raw external table pointing to Parquet files
    print("Creating raw table...")
    create_raw_table_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{RAW_TABLE} (
        DateTime timestamp,
        Bid double,
        Ask double
    )
    STORED AS PARQUET
    LOCATION 's3://{BUCKET}/{RAW_PREFIX}'
    """
    run_athena_query(create_raw_table_query)

    # Detect symbols from S3 subfolders
    print("Detecting symbols...")
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=RAW_PREFIX, Delimiter="/")
    folders = response.get("CommonPrefixes", [])
    if not folders:
        print("No symbol folders found. Exiting.")
        sys.exit(0)
    symbols = [f['Prefix'].split("/")[-2] for f in folders]
    print(f"Detected symbols: {symbols}")

    # Escape the prefix for safe regex usage
    escaped_prefix = re.escape(RAW_PREFIX).replace('/', '\\/')

    # Dictionary to store new record counts per symbol
    new_counts = {}

    # Process each symbol
    for symbol in symbols:
        # NOTE: Table name now has _spread suffix to avoid conflicts with original tables
        table_name = f"ticks_{symbol.lower()}_spread"
        s3_location = f"s3://{BUCKET}/gold/ticks_spread/{symbol}/"

        # Create Iceberg table (if not exists) – includes Spread column
        print(f"Creating Iceberg spread table for {symbol}...")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} (
            DateTime timestamp,
            Bid double,
            Ask double,
            Spread double,
            symbol string,
            year int
        )
        PARTITIONED BY (year)
        LOCATION '{s3_location}'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet'
        )
        """
        run_athena_query(create_table_query)

        # --------------------------------------------------------
        # Count how many new records exist for this symbol
        # --------------------------------------------------------
        print(f"Checking for new data for {symbol}...")
        count_query = f"""
        SELECT COUNT(*) AS new_count
        FROM (
            SELECT DISTINCT DateTime, symbol
            FROM (
                SELECT
                    DateTime,
                    regexp_extract("$path", '.*/{escaped_prefix}([^/]+)/.*', 1) AS symbol
                FROM {DATABASE}.{RAW_TABLE}
                WHERE regexp_extract("$path", '.*/{escaped_prefix}([^/]+)/.*', 1) = '{symbol}'
            )
        ) s
        LEFT JOIN {DATABASE}.{table_name} t
        ON s.DateTime = t.DateTime AND s.symbol = t.symbol
        WHERE t.DateTime IS NULL
        """
        count_query_id = run_athena_query(count_query, silent=True)
        new_count = get_count_from_query(count_query_id)
        new_counts[symbol] = new_count

        if new_count == 0:
            print(f"No new data for {symbol}.")
            continue

        print(f"Found {new_count} new distinct record(s) for {symbol}. Merging...")
        # --------------------------------------------------------
        # Insert only new data using MERGE with DISTINCT source.
        # Spread = Ask - Bid, floored at 0 (spread can never be negative).
        # --------------------------------------------------------
        merge_query = f"""
        MERGE INTO {DATABASE}.{table_name} t
        USING (
            SELECT DISTINCT
                DateTime,
                Bid,
                Ask,
                GREATEST(Ask - Bid, 0.0) AS Spread,
                regexp_extract("$path", '.*/{escaped_prefix}([^/]+)/.*', 1) AS symbol,
                year(DateTime) AS year
            FROM {DATABASE}.{RAW_TABLE}
            WHERE regexp_extract("$path", '.*/{escaped_prefix}([^/]+)/.*', 1) = '{symbol}'
        ) s
        ON t.DateTime = s.DateTime AND t.symbol = s.symbol
        WHEN NOT MATCHED THEN
            INSERT (DateTime, Bid, Ask, Spread, symbol, year)
            VALUES (s.DateTime, s.Bid, s.Ask, s.Spread, s.symbol, s.year)
        """
        run_athena_query(merge_query, silent=True)
        print(f"Inserted {new_count} new record(s) for {symbol}.")

    # ------------------------------------------------------------
    # Final summary table
    # ------------------------------------------------------------
    print("\n✅ All symbols processed successfully!")
    print("New records inserted per symbol:")
    for symbol in symbols:
        count = new_counts.get(symbol, 0)
        print(f"  {symbol:8} : {count}")
    print(f"\nDatabase: {DATABASE}")
    print(f"Raw table: {RAW_TABLE}")
    print(f"Per‑symbol Iceberg spread tables: {', '.join([f'ticks_{s.lower()}_spread' for s in symbols])}")

except Exception as e:
    print(f"\n❌ An unexpected error occurred: {e}")
    sys.exit(1)