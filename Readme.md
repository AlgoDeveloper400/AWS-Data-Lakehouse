# AWS Data Lakehouse Medallion Architecture

This project demonstrates a cloud-native **Data Lakehouse on AWS**, focused on the **Gold layer** of the Medallion Architecture. It streams financial data from the **Dukascopy API**, converts CSV to Parquet via **AWS Glue**, and exposes a Gold dataset to Engineers, Analysts, and ML/AI roles via scoped IAM.

---

## Architecture Overview

![AWS Gold Data Lakehouse Architecture](AWS%20Gold%20Data%20Lakehouse%20Architecture%20Mar%202026.png)

---

## Data Flow

Raw financial data is streamed from the **Dukascopy API** into S3 via **Kinesis Firehose**, stored as CSV. An **AWS Glue ETL job** then converts the raw CSV into Parquet format and loads it into the Gold S3 bucket. From there, Data Engineers and Analysts can query the data via **Apache Iceberg tables** using **AWS Athena** in the cloud or an on-premises **Trino query engine** locally, while ML/AI Engineers use the Gold dataset to train and deploy models.

---

## Components

### Ingestion
| Component | Description |
|---|---|
| **Dukascopy API** | Source of real-time financial market data |
| **Amazon Kinesis Firehose** | Streams and delivers data to S3 |
| **S3 Raw Bucket** | Landing zone storing raw data as CSV |

### Transformation
| Component | Description |
|---|---|
| **AWS Glue ETL Job** | Converts raw CSV to Parquet format |
| **S3 Gold Bucket** | Stores the curated, query-optimised Gold dataset |
| **S3 + Glue IAM Policies** | Scoped access policies per user/role |

### Consumption
| Component | Description |
|---|---|
| **Apache Iceberg Tables** | Partitioned, versioned table format on S3 |
| **AWS Athena** | Serverless SQL querying on the Gold dataset in the cloud |
| **On-Prem Trino Query Engine** | Local high-performance SQL analytics via Docker |
| **ML/AI Model Training** | Model training and deployment on Gold data |

---

## IAM Role Access

| Role | Access |
|---|---|
| **Data Engineer + Admin** | Full access — ingestion, raw S3, Glue ETL |
| **Data Engineer / Analyst** | Gold S3 bucket, Iceberg tables, Athena, Trino |
| **ML / AI Engineer** | Read access to Gold dataset |

---

## Tech Stack

- **Amazon Kinesis Firehose** — real-time data streaming
- **Amazon S3** — object storage for raw and gold layers
- **AWS Glue** — serverless ETL
- **Apache Iceberg** — open table format
- **AWS Athena** — serverless SQL querying (cloud)
- **Trino** — on-premises SQL query engine (local)
- **AWS IAM** — role-based access control
- **Dukascopy API** — financial market data source
