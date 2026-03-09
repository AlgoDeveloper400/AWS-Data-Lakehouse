import os
import re
import sys
import time
from pathlib import Path

import boto3
from dotenv import load_dotenv

env_path = Path(r"C:\your\base\path\AWS Data Lakehouse\.env")
if not env_path.exists():
    raise FileNotFoundError(f".env file not found at {env_path}")
load_dotenv(dotenv_path=env_path)

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

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=ATHENA_REGION
)
athena = session.client('athena')

def run_athena_query(query, silent=False):
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
        print(f"✓ Query OK: {query[:80]}...")
    return query_id

def fetch_results(query_id):
    result = athena.get_query_results(QueryExecutionId=query_id)
    rows = []
    for row in result['ResultSet']['Rows'][1:]:
        rows.append([col.get('VarCharValue', '') for col in row['Data']])
    return rows

def list_iceberg_tables():
    sql = f"SHOW TABLES IN {DATABASE}"
    qid = run_athena_query(sql, silent=True)
    rows = fetch_results(qid)
    tables = [r[0] for r in rows if r[0].startswith('ticks_')]
    return tables

def get_snapshots(table):
    sql = f"""
        SELECT snapshot_id, committed_at, operation,
               element_at(summary, 'added-records') as added,
               element_at(summary, 'deleted-records') as deleted,
               element_at(summary, 'total-records') as total
        FROM "{DATABASE}"."{table}$snapshots"
        ORDER BY committed_at
    """
    qid = run_athena_query(sql, silent=True)
    return fetch_results(qid)

def show_partitions(table, snapshot_id=None):
    if snapshot_id:
        sql = f"""
            SELECT year(datetime) as yr, count(*) as cnt
            FROM "{DATABASE}"."{table}" FOR VERSION AS OF {snapshot_id}
            GROUP BY year(datetime) ORDER BY yr
        """
    else:
        sql = f"""
            SELECT year(datetime) as yr, count(*) as cnt
            FROM "{DATABASE}"."{table}"
            GROUP BY year(datetime) ORDER BY yr
        """
    qid = run_athena_query(sql, silent=True)
    rows = fetch_results(qid)
    if not rows:
        print("  No data")
        return
    total = sum(int(r[1]) for r in rows)
    print("  Year      Rows")
    print("  " + "-"*18)
    for yr, cnt in rows:
        print(f"  {yr:<8} {int(cnt):>10,}")
    print("  " + "-"*18)
    print(f"  TOTAL     {total:>10,}")

def query_sample(table, snapshot_id=None, timestamp=None, limit=10):
    """Query first few rows at a specific snapshot or timestamp, showing ALL columns."""
    if snapshot_id:
        sql = f'SELECT * FROM "{DATABASE}"."{table}" FOR VERSION AS OF {snapshot_id} LIMIT {limit}'
        desc = f"snapshot {snapshot_id}"
    elif timestamp:
        sql = f"SELECT * FROM \"{DATABASE}\".\"{table}\" FOR TIMESTAMP AS OF TIMESTAMP '{timestamp}' LIMIT {limit}"
        desc = f"timestamp '{timestamp}'"
    else:
        sql = f'SELECT * FROM "{DATABASE}"."{table}" LIMIT {limit}'
        desc = "current"

    print(f"\n  First {limit} rows at {desc}:")
    qid = run_athena_query(sql, silent=True)

    # Read raw Athena result so we can extract the header row
    result = athena.get_query_results(QueryExecutionId=qid)
    all_rows = result['ResultSet']['Rows']

    if len(all_rows) < 2:
        print("  No rows")
        return

    # First row from Athena is always the column headers
    headers = [c.get('VarCharValue', '') for c in all_rows[0]['Data']]
    col_width = max(16, max(len(h) for h in headers) + 2)

    # Header line
    print("  " + "  ".join(h.upper().ljust(col_width) for h in headers))
    print("  " + "-" * ((col_width + 2) * len(headers)))

    # Data rows
    for row in all_rows[1:]:
        values = [c.get('VarCharValue', '') for c in row['Data']]
        print("  " + "  ".join(v.ljust(col_width) for v in values))

def diff_snapshots(table, snap_a, snap_b):
    sql_a = f"SELECT count(*) FROM \"{DATABASE}\".\"{table}\" FOR VERSION AS OF {snap_a}"
    sql_b = f"SELECT count(*) FROM \"{DATABASE}\".\"{table}\" FOR VERSION AS OF {snap_b}"
    qid_a = run_athena_query(sql_a, silent=True)
    qid_b = run_athena_query(sql_b, silent=True)
    cnt_a = int(fetch_results(qid_a)[0][0])
    cnt_b = int(fetch_results(qid_b)[0][0])
    delta = cnt_b - cnt_a
    sign = '+' if delta >= 0 else ''
    print(f"  Snapshot {snap_a}: {cnt_a:>12,} rows")
    print(f"  Snapshot {snap_b}: {cnt_b:>12,} rows")
    print(f"  Delta        : {sign}{delta:,} rows")

def rollback_to_snapshot(table, snap_id):
    print(f"\n  Rolling back {table} to snapshot {snap_id}...")
    sql = f"ALTER TABLE {DATABASE}.{table} EXECUTE rollback_to_snapshot(snapshot_id => {snap_id})"
    run_athena_query(sql, silent=False)
    print("  Rollback completed.")

def add_column(table, column_name, column_type):
    sql = f"ALTER TABLE {DATABASE}.{table} ADD COLUMNS ({column_name} {column_type})"
    run_athena_query(sql)
    print(f"  Column '{column_name}' added to {table}.")

def drop_column(table, column_name):
    sql = f"ALTER TABLE {DATABASE}.{table} DROP COLUMN {column_name}"
    run_athena_query(sql)
    print(f"  Column '{column_name}' dropped from {table}.")

def change_column(table, old_name, new_name, new_type):
    sql = f"ALTER TABLE {DATABASE}.{table} CHANGE COLUMN {old_name} {new_name} {new_type}"
    run_athena_query(sql)
    print(f"  Column '{old_name}' changed to '{new_name}' with type {new_type}.")

def set_partition_spec(table, spec):
    sql = f"ALTER TABLE {DATABASE}.{table} SET PARTITION SPEC ({spec})"
    run_athena_query(sql)
    print(f"  Partition spec for {table} set to: {spec}")

def main():
    tables = list_iceberg_tables()
    if not tables:
        print(f"No Iceberg tables with prefix 'ticks_' found in database {DATABASE}.")
        return

    print("Available tables:")
    for i, t in enumerate(tables):
        print(f"  {i}) {t}")

    while True:
        try:
            idx = int(input("\nSelect table (number): "))
            if 0 <= idx < len(tables):
                table = tables[idx]
                break
            print("Invalid number.")
        except ValueError:
            print("Please enter a number.")

    while True:
        print(f"\n{'='*60}")
        print(f"Table: {table}")
        print('='*60)
        print("1) List snapshots")
        print("2) Show current partitions")
        print("3) Query sample (current)")
        print("4) Query at snapshot")
        print("5) Query at timestamp")
        print("6) Diff two snapshots")
        print("7) Rollback to snapshot")
        print("8) Add column")
        print("9) Drop column")
        print("10) Change column")
        print("11) Set partition spec")
        print("0) Exit")
        choice = input("Choice: ").strip()

        if choice == '1':
            snaps = get_snapshots(table)
            if not snaps:
                print("No snapshots found.")
                continue
            print(f"\n{'Snapshot ID':<20} {'Committed At':<26} {'Op':<8} {'Added':>8} {'Deleted':>8} {'Total':>10}")
            print('-'*90)
            for s in snaps:
                sid, ts, op, added, deleted, total = s
                added = added or '0'
                deleted = deleted or '0'
                total = total or '?'
                print(f"{sid:<20} {ts:<26} {op:<8} {added:>8} {deleted:>8} {total:>10}")

        elif choice == '2':
            show_partitions(table)

        elif choice == '3':
            query_sample(table)

        elif choice == '4':
            snaps = get_snapshots(table)
            if not snaps:
                print("No snapshots.")
                continue
            print("Snapshots (index, snapshot_id, committed_at):")
            for i, s in enumerate(snaps):
                print(f"  {i}) {s[0]}  {s[1]}")
            try:
                idx = int(input("Select snapshot index: "))
                if 0 <= idx < len(snaps):
                    snap_id = snaps[idx][0]
                    query_sample(table, snapshot_id=snap_id)
                else:
                    print("Invalid index.")
            except ValueError:
                print("Invalid input.")

        elif choice == '5':
            ts = input("Enter timestamp (YYYY-MM-DD HH:MM:SS): ").strip()
            query_sample(table, timestamp=ts)

        elif choice == '6':
            snaps = get_snapshots(table)
            if len(snaps) < 2:
                print("Need at least two snapshots.")
                continue
            print("Snapshots:")
            for i, s in enumerate(snaps):
                print(f"  {i}) {s[0]}  {s[1]}")
            try:
                a = int(input("Index of older snapshot: "))
                b = int(input("Index of newer snapshot: "))
                if 0 <= a < len(snaps) and 0 <= b < len(snaps):
                    diff_snapshots(table, snaps[a][0], snaps[b][0])
                else:
                    print("Invalid indices.")
            except ValueError:
                print("Invalid input.")

        elif choice == '7':
            snaps = get_snapshots(table)
            if not snaps:
                print("No snapshots.")
                continue
            print("Snapshots:")
            for i, s in enumerate(snaps):
                print(f"  {i}) {s[0]}  {s[1]}")
            try:
                idx = int(input("Select snapshot index to roll back to: "))
                if 0 <= idx < len(snaps):
                    confirm = input(f"Roll back {table} to snapshot {snaps[idx][0]}? (yes/no): ")
                    if confirm.lower() == 'yes':
                        rollback_to_snapshot(table, snaps[idx][0])
                else:
                    print("Invalid index.")
            except ValueError:
                print("Invalid input.")

        elif choice == '8':
            name = input("Column name: ").strip()
            typ = input("Column type (e.g., string, int, double): ").strip()
            add_column(table, name, typ)

        elif choice == '9':
            name = input("Column name to drop: ").strip()
            confirm = input(f"Drop column '{name}' from {table}? (yes/no): ")
            if confirm.lower() == 'yes':
                drop_column(table, name)

        elif choice == '10':
            old = input("Current column name: ").strip()
            new = input("New column name: ").strip()
            typ = input("New data type: ").strip()
            change_column(table, old, new, typ)

        elif choice == '11':
            spec = input("Partition spec (e.g., 'year, month'): ").strip()
            set_partition_spec(table, spec)

        elif choice == '0':
            break

        else:
            print("Invalid choice.")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)