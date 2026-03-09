# AWS Data Lakehouse — Triple Query Engine Architecture

This project demonstrates a cloud-native **Data Lakehouse on AWS**, built around the **Gold layer** of the Medallion Architecture. It ingests financial data from a **Trading API/Platform**, converts raw CSV to Parquet via an **AWS Glue ETL job**, and exposes a curated Gold dataset to multiple consumer roles via three independent query engines, with an optional ML/AI path.

---

## Architecture Overview

![AWS Gold Data Lakehouse Architecture](AWS%20Gold%20Data%20Lakehouse%20Architecture(Triple%20Query%20Engine%20)%20Mar%202026.png)

---

## Data Flow

Raw financial data is streamed from a **Trading API/Platform** and stored as CSV in S3. An **AWS Glue ETL job** converts the raw CSV into Parquet format and loads it into the **Gold S3 bucket**. Access to both the raw bucket and Glue job is governed by **S3 + Glue IAM policies** scoped per user/role.

From the Gold dataset, three independent query engines are available:

- **QE 1 — Apache Iceberg (Cloud):** Data Engineers and Analysts query the Gold dataset via **Iceberg tables with partitioning**, enabling versioned, ACID-compliant SQL analytics directly in the cloud.
- **QE 2 — On-Premises Trino (Docker):** Data Engineers and Analysts can run high-performance SQL queries locally via a **Trino query engine** deployed in Docker, connecting back to the Gold S3 dataset.
- **QE 3 — On-Premises Spark (Docker):** Data Engineers and Analysts can run large-scale distributed analytics locally via an **Apache Spark query engine** deployed in Docker, also connecting to the Gold S3 dataset.

Additionally, **ML/AI Engineers** have read access to the Gold dataset to optionally **train and deploy ML models** — this path is separate from the three query engines.

---

## Components

### Ingestion

| Component | Description |
|---|---|
| **Trading API / Platform** | Source of real-time financial market data |
| **S3 Raw Bucket** | Landing zone storing ingested data as CSV |

### Transformation

| Component | Description |
|---|---|
| **AWS Glue ETL Job** | Converts raw CSV to Parquet format |
| **S3 Gold Bucket** | Stores the curated, query-optimised Gold dataset |
| **S3 + Glue IAM Policies** | Scoped access policies enforced per user/role |

### Consumption

| Component | Description |
|---|---|
| **Apache Iceberg Tables (QE 1)** | Partitioned, versioned table format on S3 — cloud SQL querying |
| **On-Prem Trino Query Engine (QE 2)** | Local high-performance SQL analytics via Docker |
| **On-Prem Spark Query Engine (QE 3)** | Local distributed data processing and analytics via Docker |
| **ML/AI Model Training (Optional)** | Model training and deployment on the Gold dataset |

---

## IAM Role Access

| Role | Access |
|---|---|
| **Data Engineer + Admin** | Full access — ingestion, raw S3 bucket, Glue ETL job |
| **Data Engineer / Data Analyst** | Gold S3 bucket, Iceberg tables (QE 1), Trino (QE 2), Spark (QE 3) |
| **ML / AI Engineer** | Read access to Gold S3 dataset |

---

## Tech Stack

- **Amazon S3** — object storage for raw and Gold layers
- **AWS Glue** — serverless ETL (CSV → Parquet)
- **AWS IAM** — role-based access control via S3 + Glue policies
- **Apache Iceberg** — open table format with partitioning and versioning (QE 1)
- **Trino** — on-premises SQL query engine via Docker (QE 2)
- **Apache Spark** — on-premises distributed query engine via Docker (QE 3)
- **Docker** — container runtime for on-premises query engines
- **Trading API / Platform** — financial market data source

---

## ⚠️ Disclaimer

This project is currently a work in progress. The architecture, configurations, and documentation are subject to change and have not yet been finalised. The ML/AI model training and deployment section will not be covered in this repo as a similar setup has already been implemented in a separate repository.
