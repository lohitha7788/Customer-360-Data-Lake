# Customer 360 Data Lake — Medallion Architecture on AWS

This repository contains my AWS Data Engineering Portfolio

## Project 1

A batch and streaming data lake built on AWS using the medallion (Bronze / Silver / Gold) architecture. The pipeline ingests customer data from three heterogeneous sources a transactional database, a real time clickstream and a CRM export cleans and conforms it through Glue ETL, computes RFM based churn scoring and serves the result through both a SQL data warehouse and adhoc query engine.

## Objective

Build a production style customer data lake leveraging AWS services to:

**Ingest data from three heterogeneous sources into a unified Bronze layer**
- AWS DMS performs full load + CDC replication from an RDS PostgreSQL OLTP database into S3 as Parquet
- Kinesis Data Firehose streams simulated clickstream events (~50 events/sec) into S3 as GZIP compressed JSON, partitioned by year/month/day/hour
- A daily Salesforce CRM export is simulated as CSV and uploaded directly to S3

**Automatically catalog and clean raw data through a Bronze - Silver pipeline**
- Glue Crawlers register all Bronze sources in the Glue Data Catalog for schema discovery
- Three Glue PySpark ETL jobs deduplicate, null filter, type cast and standardize each Bronze source
- Glue Data Quality rules (`EvaluateDataQuality`) enforce completeness, uniqueness and value range checks before data is allowed to reach Silver
- Silver output is written in Delta Lake format for ACID guarantees and time travel

**Compute customer level RFM features and churn risk in a Gold layer**
- A fourth Glue job joins all three Silver tables on `customer_id`
- Computes Recency, Frequency and Monetary (RFM) features per customer from order history
- Applies ruleb ased churn risk scoring (`HIGH` / `MEDIUM` / `LOW`) and RFM segment labeling (`Champions`, `At Risk`, `Loyal Customers`, etc.)
- Validates row counts and null rates before writing, failing the job if data quality regresses

**Serve the Gold layer to both a SQL warehouse and an ad-hoc query engine**
- Amazon Redshift Serverless hosts a dedicated `customer_360` table, bulk loaded via `COPY` from S3
- Amazon Athena provides adhoc SQL access directly against Bronze and Silver tables in the Glue Catalog, with saved queries for common analytics patterns

## Key Outcomes

- Full medallion pipeline processing 500 customers, 2,000 orders, 100 products and 45,000+ simulated clickstream events end to end
- Churn risk segmentation: 64% Low risk, 23% Medium risk, 13% High risk directly actionable for a retention campaign
- Change Data Capture (CDC) confirmed live via AWS DMS + PostgreSQL logical replication
- Data quality gate embedded directly in the ETL job graph, not bolted on afterward
- Dual format Gold output (Delta Lake for the lake, plain Parquet export for warehouse compatibility) to solve real Delta vs Redshift Spectrum incompatibilities

## Deliverables

- Working RDS - DMS - S3 (Bronze) pipeline with full load + CDC
- Kinesis Firehose clickstream ingestion pipeline
- 5 Glue PySpark ETL jobs (3× Bronze→Silver, 1× Silver→Gold, embedded Data Quality rules)
- Glue Crawlers + Data Catalog covering Bronze and Silver layers
- Redshift Serverless warehouse with `COPY`loaded Customer 360 table
- 5 saved Athena queries for adhoc analytics
- Glue Triggers for scheduled (hourly Silver, daily Gold) automation
- README with setup instructions

## System Architecture


## Prerequisites

- AWS account with access to RDS, DMS, S3, Kinesis Firehose, Glue, Redshift Serverless, Athena and IAM
- Python 3.11+ for local data generator scripts
- `psycopg2`, `boto3`, `pandas`, `faker` Python libraries
- AWS CLI configured with non root IAM credentials
- Single region used throughout: `ca-central-1`

## Data Ingestion

### Transactional data — RDS PostgreSQL + AWS DMS
- RDS instance: `customer360-postgres` (PostgreSQL 15, `db.t3.micro`, free tier)
- Tables: `customers` (500 rows), `orders` (2,000 rows), `products` (100 rows), seeded via `setup_rds.py`
- `rds.logical_replication` enabled via a custom parameter group to support DMS Change Data Capture
- DMS replication instance (`customer360-replication`, `dms.t3.micro`) performs full load + ongoing CDC into S3 as Parquet, one prefix per source table

### Clickstream data — Kinesis Data Firehose
- Delivery stream: `customer360-clickstream`, Direct PUT source, Amazon S3 destination
- `clickstream_producer.py` simulates ~50 events/second across 7 event types (page_view, add_to_cart, purchase, etc.)
- Buffered at 5 MB / 60 seconds, GZIP-compressed, partitioned by `year/month/day/hour`

### CRM data — Simulated Salesforce export
- `salesforce_csv_generator.py` generates 500 synthetic customer profiles (segment, lifetime value, marketing opt-in, etc.)
- Uploaded directly to `s3://<bucket>/bronze/salesforce-crm/`

## Data Catalog — Glue Crawlers

- Database: `customer360_db`
- Bronze crawlers registered 5 tables: `bronze_orders`, `bronze_customers`, `bronze_products` (DMS auto-split by source table), `bronze_clickstream`, `bronze_salesforce_crm`
- Silver Delta tables were registered manually via DDL + `MSCK REPAIR TABLE` rather than crawled, since standard Glue Crawlers do not reliably interpret the Delta Lake transaction log (`_delta_log/`)

## Transformation — Glue ETL (Bronze → Silver)

Three PySpark jobs, one per source, each following the same pattern:

1. Read from the Glue Data Catalog via `create_dynamic_frame.from_catalog`
2. Deduplicate on primary key, filter nulls on key columns, cast types
3. Run `EvaluateDataQuality` (Glue Data Quality) — job fails if any rule fails
4. Write to `s3://<bucket>/silver/<table>/` in **Delta Lake format**, partitioned where relevant

| Job | Source | Key Transformations |
|---|---|---|
| `bronze-to-silver-orders` | `bronze_orders` | Dedup on `order_id`, cast `total_amount` to double, uppercase `status`, derive `order_year`/`order_month` |
| `bronze-to-silver-clickstream` | `bronze_clickstream` | Dedup on `event_id`, derive `event_date`/`event_hour` from timestamp |
| `bronze-to-silver-crm` | `bronze_salesforce_crm` | Dedup on `customer_id`, lowercase/trim email, cast `lifetime_value` to double |

**Data Quality rules (orders):** `IsComplete` on `order_id`/`customer_id`/`order_date`, `ColumnValues total_amount > 0`, `IsUnique order_id`, `ColumnValues status in [COMPLETED, PENDING, RETURNED]`.

## Aggregation — Glue ETL (Silver → Gold)

`silver-to-gold-customer360` joins all three Silver Delta tables on `customer_id` and computes:

- **Recency** — days since last order
- **Frequency** — total order count
- **Monetary** — total amount spent
- **Churn risk** — rule-based (`HIGH` if recency > 90 days and frequency < 3, `MEDIUM` if recency > 60 and frequency < 5, else `LOW`)
- **RFM segment** — `Champions`, `At Risk`, `Loyal Customers`, `Recent Customers`, `Needs Attention`
- Clickstream engagement features — total clicks, sessions, active days

Before writing, the job validates:
- Gold row count is non zero and does not exceed the CRM source row count (guards against join fan-out)
- Null rate on `customer_id`, `email`, `churn_risk`, `rfm_segment`  job fails on any null `customer_id`

Output is written twice:
- `gold/customer-360/` — fullfidelity Delta Lake table (for lake consumers)
- `gold/customer-360-export/`  a 17-column, explicitly typed plain Parquet export matching the Redshift DDL exactly (Redshift Spectrum's Parquet reader does not read Delta transaction logs and enforces strict column count/type matching)

## Warehouse — Amazon Redshift Serverless

- Namespace: `customer360-namespace`, Workgroup: `customer360-workgroup` (8 base RPUs)
- Table `customer360.customer_360` created via DDL with `DISTKEY(customer_id)` and `SORTKEY(churn_risk, rfm_segment)`
- Loaded via `COPY ... FROM 's3://<bucket>/gold/customer-360-export/' IAM_ROLE '...' FORMAT AS PARQUET`   500 rows loaded
- IAM role `redshift-s3-role` grants read-only, bucket-scoped S3 access
- $20/month usage limit configured to bound Serverless RPU-hour spend

## Adhoc Analytics — Amazon Athena

Query result location: `s3://<bucket>/athena-results/`. Five saved queries against `customer360_db`:

1. `top-customers-by-value` — top 10 customers by completed order spend (Bronze)
2. `clickstream-events-today` — event volume by type (Bronze)
3. `customers-by-segment` — customer count and avg. lifetime value by CRM segment (Bronze)
4. `silver-orders-quality` — row/null-rate sanity check (Silver)
5. `monthly-revenue-trend` — order count and revenue by year/month (Silver)

## Automation — Glue Triggers

- `hourly-bronze-to-silver` — scheduled trigger firing all three Bronze→Silver jobs hourly
- `daily-gold-refresh` — scheduled trigger firing the Gold job daily at 2:00 AM UTC

## Data Flow

1. **Ingestion**: DMS replicates PostgreSQL → S3 Bronze (Parquet); Firehose streams clickstream - S3 Bronze (GZIP JSON); CSV generator uploads CRM export - S3 Bronze (CSV)
2. **Cataloging**: Glue Crawlers register Bronze schemas in the Glue Data Catalog
3. **Cleaning**: Glue ETL jobs deduplicate, type cast and quality check each Bronze source, writing Delta format Silver tables
4. **Aggregation**: A join across all three Silver tables computes RFM features and churn risk, writing a validated Gold table (Delta + Parquet export)
5. **Serving**: Redshift Serverless is bulk loaded from the Gold export via `COPY`; Athena queries Bronze/Silver directly for ad hoc analysis

## Security & Compliance

- **Encryption**: S3 buckets use SSE-S3; RDS and Redshift Serverless connections require SSL/TLS
- **IAM Roles**: Least privilege, service specific roles — `glue-etl-role` (Glue → S3/Catalog), `redshift-s3-role` (Redshift → S3, read-only, bucket-scoped), `firehose-s3-role` (Firehose → S3), `DMSS3AccessRole-*` (DMS → S3)
- **Network**: RDS and DMS deployed in the default VPC with security-group-scoped inbound access on port 5432
- **Access Control**: RDS uses password authentication with SSL enforced; Redshift authenticates via IAM-managed admin credentials

## Monitoring & Data Quality

- **CloudWatch Logs**: DMS task logs (`/aws-glue/jobs` for Glue, DMS task server logs) used throughout development to diagnose connection, schema and replication slot failures
- **Glue Job Bookmarks**: Enabled on all ETL jobs to support incremental reprocessing
- **Glue Data Quality**: `EvaluateDataQuality` enforced inline in the orders Silver job — job fails closed if any rule fails
- **Gold-layer validation**: Row-count and null rate checks executed before every Gold write, raising an exception (halting the job, preventing a bad Gold publish) on failure

## Environment Variables / Key Identifiers

```
REGION: ca-central-1
S3_BUCKET: customer-360-{account-id}
GLUE_DATABASE: customer360_db
RDS_ENDPOINT: customer360-postgres.<id>.ca-central-1.rds.amazonaws.com
REDSHIFT_WORKGROUP: customer360-workgroup
REDSHIFT_NAMESPACE: customer360-namespace
REDSHIFT_DATABASE: dev
```

## Running the Local Generators

```bash
pip install psycopg2-binary boto3 pandas faker

# Seed the RDS source database
python setup_rds.py

# Stream simulated clickstream events (Ctrl+C to stop)
python clickstream_producer.py

# Generate and upload a daily Salesforce CRM export
python salesforce_csv_generator.py
```

## Results

- Ingested and processed 500 customers, 2,000 orders, 100 products, and 45,000+ clickstream events end-to-end through Bronze → Silver → Gold
- Delivered a validated Gold table with RFM features and churn risk for all 500 customers (321 Low / 114 Medium / 65 High risk)
- Loaded the full Gold dataset into Redshift Serverless via `COPY`, verified with row-count and cohort-breakdown queries
- Diagnosed and resolved multiple real world AWS integration issues, including DMS table-selection schema mismatches, missing PostgreSQL logical replication configuration, Delta Lake vs. Redshift Spectrum Parquet incompatibilities (column count, column types, and stale files from `overwrite` mode), and Athena partition-discovery gaps
- Delivered a serverless first, cost-conscious architecture with scheduled (but opt-in) automation via Glue Triggers

## Cost Estimate

| Service | Approx. Cost |
|---|---|
| RDS PostgreSQL (`db.t3.micro`) | ~$0.017/hr |
| DMS Replication Instance (`dms.t3.micro`) | ~$0.018/hr |
| Kinesis Data Firehose | ~$0.029/GB ingested |
| AWS Glue ETL (G.1X, per job run) | ~$0.44/DPU-hr |
| Redshift Serverless (8 base RPUs) | ~$0.36/RPU-hr while active |
| Amazon S3 | ~$0.023/GB (Standard) |
| Amazon Athena | ~$5.00/TB scanned |
##Next Steps
	-	QuickSight Dashboard  build a Customer 360 dashboard on top of the Redshift customer_360 table, covering an RFM segment breakdown (pie chart), a recency vs monetary scatter plot colored by churn ris, and a churn risk cohort summary table. Deferred for now in favor of validating the pipeline end to end via Redshift/Athena SQL first.
## Lessons Learned

- DMS's "Schema" selection rule must match the actual PostgreSQL schema (typically `public`), not the database name  a subtle but easy mistake since both often share a similar looking value
- AWS DMS Change Data Capture requires `rds.logical_replication = 1` on the source RDS parameter group and an instance reboot  without it, full load succeeds but CDC fails silently at task start with a `wal_level` error
- Delta Lake's `overwrite` mode does not delete superseded Parquet files immediately — tools that read the raw files directly (Athena via `MSCK REPAIR`, Redshift Spectrum) will double count rows until `VACUUM` is run
- Redshift Spectrum's Parquet `COPY` performs strict column count and column type matching against the target table; it also does not understand Delta's `_delta_log/` and will attempt to parse the transaction log JSON as data , the practical fix is a dedicated, explicitly typed plain Parquet export path alongside the Delta table
- Standard Glue Crawlers do not reliably detect Delta Lake tables out of the box; they will crawl the `_delta_log/` and each Hive style partition folder as separate, incorrect tables  manual DDL + `MSCK REPAIR TABLE` was more reliable for exposing Delta data to Athena
- IAM permission errors during interactive console workflows (e.g., Redshift Serverless auto creating an IAM role) surface as generic validation errors rather than naming the missing permission explicitly  creating the role manually via IAM and associating it afterward is a reliable workaround
