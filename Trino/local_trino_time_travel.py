import os
import sys
from pathlib import Path

import trino
from dotenv import load_dotenv

# ── Load .env ──────────────────────────────────────────────────────────────────
env_path = Path(r"C:\your\base\path\AWS Data Lakehouse\Query Engine Setup\.env")
if not env_path.exists():
    raise FileNotFoundError(f".env file not found at {env_path}")
load_dotenv(dotenv_path=env_path)

TRINO_HOST     = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT     = int(os.getenv("TRINO_PORT", "8081"))
TRINO_USER     = os.getenv("TRINO_USER", "trino")
TRINO_CATALOG  = os.getenv("TRINO_CATALOG")   # e.g. "iceberg"
TRINO_SCHEMA   = os.getenv("TRINO_SCHEMA")    # your database/schema name

if not TRINO_CATALOG:
    raise ValueError("TRINO_CATALOG must be set in the .env file (e.g., 'iceberg').")
if not TRINO_SCHEMA:
    raise ValueError("TRINO_SCHEMA must be set in the .env file (e.g., 'gold').")

# ── Trino connection ───────────────────────────────────────────────────────────
def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )

def run_query(sql, silent=False):
    """Execute a SQL statement and return (cursor, rows)."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        if not silent:
            print(f"✓ Query OK: {sql[:80]}...")
        return cur, rows
    except Exception as e:
        raise Exception(f"Trino query failed:\n  SQL : {sql}\n  Error: {e}")
    finally:
        conn.close()

# ── Helpers ────────────────────────────────────────────────────────────────────
def list_iceberg_tables():
    _, rows = run_query(f"SHOW TABLES FROM {TRINO_CATALOG}.{TRINO_SCHEMA}", silent=True)
    return [r[0] for r in rows if r[0].startswith("ticks_")]

def get_snapshots(table):
    sql = f"""
        SELECT snapshot_id, committed_at, operation,
               summary['added-records']   AS added,
               summary['deleted-records'] AS deleted,
               summary['total-records']   AS total
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}."{table}$snapshots"
        ORDER BY committed_at
    """
    _, rows = run_query(sql, silent=True)
    return rows

def show_partitions(table, snapshot_id=None):
    if snapshot_id:
        sql = f"""
            SELECT year(datetime) AS yr, count(*) AS cnt
            FROM {TRINO_CATALOG}.{TRINO_SCHEMA}."{table}"
            FOR VERSION AS OF {snapshot_id}
            GROUP BY year(datetime) ORDER BY yr
        """
    else:
        sql = f"""
            SELECT year(datetime) AS yr, count(*) AS cnt
            FROM {TRINO_CATALOG}.{TRINO_SCHEMA}."{table}"
            GROUP BY year(datetime) ORDER BY yr
        """
    _, rows = run_query(sql, silent=True)
    if not rows:
        print("  No data")
        return
    total = sum(int(r[1]) for r in rows)
    print("  Year      Rows")
    print("  " + "-" * 18)
    for yr, cnt in rows:
        print(f"  {yr:<8} {int(cnt):>10,}")
    print("  " + "-" * 18)
    print(f"  TOTAL     {total:>10,}")

def query_sample(table, snapshot_id=None, timestamp=None, limit=10):
    if snapshot_id:
        sql  = f'SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}."{table}" FOR VERSION AS OF {snapshot_id} LIMIT {limit}'
        desc = f"snapshot {snapshot_id}"
    elif timestamp:
        sql  = f"SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.\"{table}\" FOR TIMESTAMP AS OF TIMESTAMP '{timestamp}' LIMIT {limit}"
        desc = f"timestamp '{timestamp}'"
    else:
        sql  = f'SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}."{table}" LIMIT {limit}'
        desc = "current"

    print(f"\n  First {limit} rows at {desc}:")
    cur, rows = run_query(sql, silent=True)

    if not rows:
        print("  No rows")
        return

    headers   = [d[0] for d in cur.description]
    col_width = max(16, max(len(h) for h in headers) + 2)

    print("  " + "  ".join(h.upper().ljust(col_width) for h in headers))
    print("  " + "-" * ((col_width + 2) * len(headers)))
    for row in rows:
        values = [str(v) if v is not None else "" for v in row]
        print("  " + "  ".join(v.ljust(col_width) for v in values))

def diff_snapshots(table, snap_a, snap_b):
    _, r_a = run_query(f'SELECT count(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}."{table}" FOR VERSION AS OF {snap_a}', silent=True)
    _, r_b = run_query(f'SELECT count(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}."{table}" FOR VERSION AS OF {snap_b}', silent=True)
    cnt_a  = int(r_a[0][0])
    cnt_b  = int(r_b[0][0])
    delta  = cnt_b - cnt_a
    sign   = "+" if delta >= 0 else ""
    print(f"  Snapshot {snap_a}: {cnt_a:>12,} rows")
    print(f"  Snapshot {snap_b}: {cnt_b:>12,} rows")
    print(f"  Delta        : {sign}{delta:,} rows")

def rollback_to_snapshot(table, snap_id):
    print(f"\n  Rolling back {table} to snapshot {snap_id}...")
    sql = f"ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{table} EXECUTE rollback_to_snapshot({snap_id})"
    run_query(sql)
    print("  Rollback completed.")

def add_column(table, column_name, column_type):
    run_query(f"ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{table} ADD COLUMN {column_name} {column_type}")
    print(f"  Column '{column_name}' added to {table}.")

def drop_column(table, column_name):
    run_query(f"ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{table} DROP COLUMN {column_name}")
    print(f"  Column '{column_name}' dropped from {table}.")

def change_column(table, old_name, new_name, new_type):
    # Trino: rename first, then retype if needed
    run_query(f"ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{table} RENAME COLUMN {old_name} TO {new_name}")
    if new_type:
        run_query(f"ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{table} ALTER COLUMN {new_name} SET DATA TYPE {new_type}")
    print(f"  Column '{old_name}' → '{new_name}' ({new_type}) updated.")

def set_partition_spec(table, spec):
    run_query(f"ALTER TABLE {TRINO_CATALOG}.{TRINO_SCHEMA}.{table} SET PROPERTIES partitioning = ARRAY[{spec}]")
    print(f"  Partition spec for {table} set to: {spec}")

# ── Main menu ──────────────────────────────────────────────────────────────────
def main():
    tables = list_iceberg_tables()
    if not tables:
        print(f"No Iceberg tables with prefix 'ticks_' found in {TRINO_CATALOG}.{TRINO_SCHEMA}.")
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
        print(f"Table : {TRINO_CATALOG}.{TRINO_SCHEMA}.{table}")
        print(f"Engine: Trino @ {TRINO_HOST}:{TRINO_PORT}")
        print("=" * 60)
        print("1)  List snapshots")
        print("2)  Show current partitions")
        print("3)  Query sample (current)")
        print("4)  Query at snapshot")
        print("5)  Query at timestamp")
        print("6)  Diff two snapshots")
        print("7)  Rollback to snapshot")
        print("8)  Add column")
        print("9)  Drop column")
        print("10) Change column")
        print("11) Set partition spec")
        print("0)  Exit")
        choice = input("Choice: ").strip()

        if choice == "1":
            snaps = get_snapshots(table)
            if not snaps:
                print("No snapshots found.")
                continue
            print(f"\n{'Snapshot ID':<20} {'Committed At':<26} {'Op':<8} {'Added':>8} {'Deleted':>8} {'Total':>10}")
            print("-" * 90)
            for s in snaps:
                sid, ts, op, added, deleted, total = s
                print(f"{str(sid):<20} {str(ts):<26} {str(op):<8} {str(added or 0):>8} {str(deleted or 0):>8} {str(total or '?'):>10}")

        elif choice == "2":
            show_partitions(table)

        elif choice == "3":
            query_sample(table)

        elif choice == "4":
            snaps = get_snapshots(table)
            if not snaps:
                print("No snapshots.")
                continue
            for i, s in enumerate(snaps):
                print(f"  {i}) {s[0]}  {s[1]}")
            try:
                idx = int(input("Select snapshot index: "))
                if 0 <= idx < len(snaps):
                    query_sample(table, snapshot_id=snaps[idx][0])
                else:
                    print("Invalid index.")
            except ValueError:
                print("Invalid input.")

        elif choice == "5":
            ts = input("Enter timestamp (YYYY-MM-DD HH:MM:SS): ").strip()
            query_sample(table, timestamp=ts)

        elif choice == "6":
            snaps = get_snapshots(table)
            if len(snaps) < 2:
                print("Need at least two snapshots.")
                continue
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

        elif choice == "7":
            snaps = get_snapshots(table)
            if not snaps:
                print("No snapshots.")
                continue
            for i, s in enumerate(snaps):
                print(f"  {i}) {s[0]}  {s[1]}")
            try:
                idx = int(input("Select snapshot index to roll back to: "))
                if 0 <= idx < len(snaps):
                    confirm = input(f"Roll back {table} to snapshot {snaps[idx][0]}? (yes/no): ")
                    if confirm.lower() == "yes":
                        rollback_to_snapshot(table, snaps[idx][0])
                else:
                    print("Invalid index.")
            except ValueError:
                print("Invalid input.")

        elif choice == "8":
            name = input("Column name: ").strip()
            typ  = input("Column type (e.g., VARCHAR, BIGINT, DOUBLE): ").strip()
            add_column(table, name, typ)

        elif choice == "9":
            name    = input("Column name to drop: ").strip()
            confirm = input(f"Drop column '{name}' from {table}? (yes/no): ")
            if confirm.lower() == "yes":
                drop_column(table, name)

        elif choice == "10":
            old = input("Current column name: ").strip()
            new = input("New column name: ").strip()
            typ = input("New data type (leave blank to skip retype): ").strip()
            change_column(table, old, new, typ)

        elif choice == "11":
            print("Example spec: 'year(datetime)', 'month(datetime)'")
            spec = input("Partition spec (comma-separated Trino expressions): ").strip()
            set_partition_spec(table, spec)

        elif choice == "0":
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)