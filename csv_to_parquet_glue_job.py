import sys
import logging

import boto3
from botocore.exceptions import ClientError

from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import to_timestamp

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Job parameters — only BUCKET is required, everything else is hardcoded
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

BUCKET         = "Yourawsbucket"
RAW_PREFIX     = "source folder"
PARQUET_PREFIX = "output folder"

# ---------------------------------------------------------------------------
# Spark / Glue initialisation
# ---------------------------------------------------------------------------

sc       = SparkContext()
glue_ctx = GlueContext(sc)
spark    = glue_ctx.spark_session
job      = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# Schema — read DateTime as string first, then cast to timestamp below.
# The CSV format is: 2026-03-07 07:50:01.123
# ---------------------------------------------------------------------------

TICK_SCHEMA = StructType([
    StructField("DateTime", StringType(), nullable=False),
    StructField("Bid",      DoubleType(), nullable=False),
    StructField("Ask",      DoubleType(), nullable=False),
])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def list_csv_files(s3_client, bucket, prefix):
    """
    Return all .csv object keys found under prefix/.
    Example output: ['raw/BTCUSD/BTCUSD_20260307_0750.csv', ...]
    """
    keys      = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                keys.append(key)
    return keys


def csv_key_to_parquet_key(csv_key, raw_prefix, parquet_prefix):
    """
    Swap the top-level prefix and file extension to derive the output key.

    raw/BTCUSD/BTCUSD_20260307_0750.csv
        -> Training Batch/BTCUSD/BTCUSD_20260307_0750.parquet
    """
    relative         = csv_key[len(raw_prefix) + 1:]
    relative_parquet = relative.rsplit(".", 1)[0] + ".parquet"
    return "{}/{}".format(parquet_prefix, relative_parquet)


def parquet_exists(s3_client, bucket, key):
    """Return True if the target parquet file already exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    s3_client = boto3.client("s3")

    log.info("Bucket      : %s", BUCKET)
    log.info("Source      : %s/", RAW_PREFIX)
    log.info("Destination : %s/", PARQUET_PREFIX)
    log.info("Scanning for CSV files...")

    all_csv_keys = list_csv_files(s3_client, BUCKET, RAW_PREFIX)
    log.info("Found %d CSV file(s).", len(all_csv_keys))

    if not all_csv_keys:
        log.info("Nothing to process. Exiting.")
        return

    converted = 0
    skipped   = 0
    failed    = 0

    for csv_key in sorted(all_csv_keys):

        parquet_key = csv_key_to_parquet_key(csv_key, RAW_PREFIX, PARQUET_PREFIX)

        # ------------------------------------------------------------
        # Idempotency — skip if the parquet file already exists
        # ------------------------------------------------------------
        if parquet_exists(s3_client, BUCKET, parquet_key):
            log.info("SKIP     %s  (already converted)", parquet_key)
            skipped += 1
            continue

        csv_s3_path     = "s3://{}/{}".format(BUCKET, csv_key)
        parquet_s3_path = "s3://{}/{}".format(BUCKET, parquet_key)

        log.info("CONVERT  %s", csv_key)
        log.info("      -> %s", parquet_key)

        try:
            # --------------------------------------------------------
            # Read CSV with explicit schema
            # --------------------------------------------------------
            df = (
                spark.read
                .option("header", "true")
                .option("enforceSchema", "true")
                .schema(TICK_SCHEMA)
                .csv(csv_s3_path)
            )

            # Cast DateTime from string to proper timestamp so Parquet
            # stores it as timestamp(3), matching the Athena table schema.
            # CSV format written by mt5_to_s3.py: 2026-03-07 07:50:01.123
            df = df.withColumn(
                "DateTime",
                to_timestamp("DateTime", "yyyy-MM-dd HH:mm:ss.SSS")
            )

            # --------------------------------------------------------
            # Write to a temp S3 path first.
            # coalesce(1) produces a single part file per window.
            # --------------------------------------------------------
            tmp_s3_path = parquet_s3_path + "_tmp"
            tmp_prefix  = parquet_key + "_tmp/"

            df.coalesce(1).write.mode("overwrite").parquet(tmp_s3_path)

            # --------------------------------------------------------
            # Find the part file Spark wrote and copy it to the
            # exact named key e.g. BTCUSD_20260307_0750.parquet
            # --------------------------------------------------------
            response   = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=tmp_prefix)
            part_files = [
                obj["Key"] for obj in response.get("Contents", [])
                if obj["Key"].endswith(".parquet")
            ]

            if not part_files:
                raise RuntimeError(
                    "No .parquet part file found in temp path: {}".format(tmp_s3_path)
                )

            s3_client.copy_object(
                Bucket     = BUCKET,
                CopySource = {"Bucket": BUCKET, "Key": part_files[0]},
                Key        = parquet_key,
            )

            # --------------------------------------------------------
            # Clean up temp folder
            # --------------------------------------------------------
            for obj in response.get("Contents", []):
                s3_client.delete_object(Bucket=BUCKET, Key=obj["Key"])

            log.info("OK       s3://%s/%s", BUCKET, parquet_key)
            converted += 1

        except Exception as exc:
            log.error("FAILED   %s  --  %s", csv_key, str(exc))
            failed += 1
            continue

    # ------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------
    log.info("-" * 55)
    log.info("Job complete.")
    log.info("  Converted : %d", converted)
    log.info("  Skipped   : %d  (already existed)", skipped)
    log.info("  Failed    : %d", failed)
    log.info("  Total     : %d", len(all_csv_keys))
    log.info("-" * 55)

    if failed > 0:
        raise RuntimeError("{} file(s) failed -- check the logs above.".format(failed))


main()
job.commit()