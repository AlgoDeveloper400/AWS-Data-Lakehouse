import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# ── Load .env ──────────────────────────────────────────────────────────────────
env_path = Path(r"C:\your\base\path\AWS Data Lakehouse\Query Engine Setup\.env")
if not env_path.exists():
    raise FileNotFoundError(f".env file not found at {env_path}")
load_dotenv(dotenv_path=env_path)

AWS_ACCESS_KEY  = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY  = os.getenv("AWS_SECRET_KEY")
AWS_REGION      = os.getenv("AWS_REGION")
S3_WAREHOUSE    = os.getenv("S3_WAREHOUSE_PATH")
SCHEMA          = os.getenv("TRINO_SCHEMA")
CATALOG         = "glue_catalog"

# ── Validate ───────────────────────────────────────────────────────────────────
required = {
    "AWS_ACCESS_KEY":    AWS_ACCESS_KEY,
    "AWS_SECRET_KEY":    AWS_SECRET_KEY,
    "AWS_REGION":        AWS_REGION,
    "S3_WAREHOUSE_PATH": S3_WAREHOUSE,
    "TRINO_SCHEMA":      SCHEMA,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise ValueError(f"Missing env vars: {', '.join(missing)}\nCheck your .env at: {env_path}")

# ── Remap env vars to standard AWS SDK names so Glue client can find them ──────
os.environ["AWS_ACCESS_KEY_ID"]     = AWS_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_KEY
os.environ["AWS_REGION"]            = AWS_REGION
os.environ["AWS_DEFAULT_REGION"]    = AWS_REGION

# ── JARs (Iceberg 1.10.1 for Spark 4.0 Scala 2.13) ───────────────────────────
ICEBERG_JAR = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1"
AWS_JAR     = "org.apache.iceberg:iceberg-aws-bundle:1.10.1"

# ── Spark Session ──────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("iceberg-lakehouse") \
    .master("spark://localhost:7077") \
    \
    .config("spark.jars.packages", f"{ICEBERG_JAR},{AWS_JAR}") \
    \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    \
    .config("spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse",   S3_WAREHOUSE) \
    .config("spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO") \
    \
    .config("spark.hadoop.fs.s3a.access.key",   AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",   AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.region",       AWS_REGION) \
    .config("spark.hadoop.fs.s3a.endpoint",     "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    \
    .config("spark.driver.extraJavaOptions",
            f"-Daws.accessKeyId={AWS_ACCESS_KEY} "
            f"-Daws.secretAccessKey={AWS_SECRET_KEY} "
            f"-Daws.region={AWS_REGION}") \
    .config("spark.executor.extraJavaOptions",
            f"-Daws.accessKeyId={AWS_ACCESS_KEY} "
            f"-Daws.secretAccessKey={AWS_SECRET_KEY} "
            f"-Daws.region={AWS_REGION}") \
    \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"\n✅ Spark 4.0 connected → {CATALOG}.{SCHEMA}")

# ── Helpers ────────────────────────────────────────────────────────────────────

def list_tables():
    """List all tables in your gold schema."""
    spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show(truncate=False)


def query_table(table, limit=10):
    """Query current state of a table."""
    return spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{table} LIMIT {limit}")


def show_snapshots(table):
    """List all snapshots for a table."""
    return spark.sql(f"""
        SELECT snapshot_id, committed_at, operation, summary
        FROM {CATALOG}.{SCHEMA}.{table}.snapshots
        ORDER BY committed_at
    """)


def time_travel_by_snapshot(table, snapshot_id):
    """Read table at a specific snapshot ID."""
    return spark.read \
        .option("snapshot-id", snapshot_id) \
        .table(f"{CATALOG}.{SCHEMA}.{table}")


def time_travel_by_timestamp(table, timestamp):
    """Read table as it was at a given timestamp.
       Format: 'YYYY-MM-DD HH:MM:SS'
    """
    return spark.read \
        .option("as-of-timestamp", timestamp) \
        .table(f"{CATALOG}.{SCHEMA}.{table}")


def show_partitions(table):
    """Show row counts grouped by year partition."""
    return spark.sql(f"""
        SELECT year(datetime) AS yr, count(*) AS cnt
        FROM {CATALOG}.{SCHEMA}.{table}
        GROUP BY year(datetime)
        ORDER BY yr
    """)


def diff_snapshots(table, snap_a, snap_b):
    """Compare row counts between two snapshots."""
    cnt_a = spark.read.option("snapshot-id", snap_a) \
        .table(f"{CATALOG}.{SCHEMA}.{table}").count()
    cnt_b = spark.read.option("snapshot-id", snap_b) \
        .table(f"{CATALOG}.{SCHEMA}.{table}").count()
    delta = cnt_b - cnt_a
    sign  = "+" if delta >= 0 else ""
    print(f"  Snapshot {snap_a}: {cnt_a:>12,} rows")
    print(f"  Snapshot {snap_b}: {cnt_b:>12,} rows")
    print(f"  Delta          : {sign}{delta:,} rows")


def rollback_to_snapshot(table, snapshot_id):
    """Roll back a table to a specific snapshot."""
    spark.sql(f"""
        CALL {CATALOG}.system.rollback_to_snapshot(
            '{SCHEMA}.{table}', {snapshot_id}
        )
    """)
    print(f"  ✅ Rolled back {table} to snapshot {snapshot_id}")


def add_column(table, column_name, column_type):
    """Add a new column to a table."""
    spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.{table} ADD COLUMN {column_name} {column_type}")
    print(f"  ✅ Column '{column_name}' added.")


def drop_column(table, column_name):
    """Drop a column from a table."""
    spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.{table} DROP COLUMN {column_name}")
    print(f"  ✅ Column '{column_name}' dropped.")


def expire_snapshots(table, older_than):
    """Clean up old snapshots to save S3 storage costs."""
    spark.sql(f"""
        CALL {CATALOG}.system.expire_snapshots(
            '{SCHEMA}.{table}',
            TIMESTAMP '{older_than}'
        )
    """)
    print(f"  ✅ Snapshots older than {older_than} expired.")


def rewrite_files(table):
    """Compact small files — big performance boost after many writes."""
    spark.sql(f"""
        CALL {CATALOG}.system.rewrite_data_files('{SCHEMA}.{table}')
    """)
    print(f"  ✅ Data files compacted for {table}.")


# ── Main menu ──────────────────────────────────────────────────────────────────
def main():
    print("\nAvailable tables:")
    list_tables()

    table = input("\nEnter table name: ").strip()

    while True:
        print(f"\n{'='*60}")
        print(f"Table : {CATALOG}.{SCHEMA}.{table}")
        print(f"Engine: Spark 4.0 @ localhost:7077")
        print("="*60)
        print("1)  List snapshots")
        print("2)  Show partitions")
        print("3)  Query sample (current)")
        print("4)  Time travel by snapshot ID")
        print("5)  Time travel by timestamp")
        print("6)  Diff two snapshots")
        print("7)  Rollback to snapshot")
        print("8)  Add column")
        print("9)  Drop column")
        print("10) Expire old snapshots")
        print("11) Compact small files")
        print("0)  Exit")
        choice = input("Choice: ").strip()

        if choice == "1":
            show_snapshots(table).show(truncate=False)

        elif choice == "2":
            show_partitions(table).show()

        elif choice == "3":
            query_table(table).show(truncate=False)

        elif choice == "4":
            snap_id = input("Snapshot ID: ").strip()
            time_travel_by_snapshot(table, int(snap_id)).show(truncate=False)

        elif choice == "5":
            ts = input("Timestamp (YYYY-MM-DD HH:MM:SS): ").strip()
            time_travel_by_timestamp(table, ts).show(truncate=False)

        elif choice == "6":
            snap_a = int(input("Older snapshot ID: ").strip())
            snap_b = int(input("Newer snapshot ID: ").strip())
            diff_snapshots(table, snap_a, snap_b)

        elif choice == "7":
            snap_id = input("Snapshot ID to roll back to: ").strip()
            confirm = input(f"Roll back {table} to snapshot {snap_id}? (yes/no): ")
            if confirm.lower() == "yes":
                rollback_to_snapshot(table, int(snap_id))

        elif choice == "8":
            name = input("Column name: ").strip()
            typ  = input("Column type (e.g. STRING, BIGINT, DOUBLE): ").strip()
            add_column(table, name, typ)

        elif choice == "9":
            name    = input("Column name to drop: ").strip()
            confirm = input(f"Drop '{name}' from {table}? (yes/no): ")
            if confirm.lower() == "yes":
                drop_column(table, name)

        elif choice == "10":
            ts = input("Expire snapshots older than (YYYY-MM-DD HH:MM:SS): ").strip()
            expire_snapshots(table, ts)

        elif choice == "11":
            rewrite_files(table)

        elif choice == "0":
            break

        else:
            print("Invalid choice.")

    spark.stop()
    print("\n👋 Spark session closed.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting.")
        spark.stop()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        spark.stop()