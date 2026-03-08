# SaaS Analytics Cloud Pipeline

## 📌 Overview

This module implements a **cloud-based analytics pipeline** that
processes simulated SaaS subscription data using a modern data stack.

The pipeline demonstrates a production-style workflow including:

-   synthetic SaaS data generation
-   object storage ingestion via Cloudflare R2
-   cloud data warehouse loading into Snowflake
-   layered analytics transformations with dbt
-   scheduled orchestration with Apache Airflow
-   pipeline monitoring and alerts via Telegram

This stack represents the **Year 2 phase of the project**, where the
pipeline evolves from a local development environment into a
**cloud-native analytics architecture**.

------------------------------------------------------------------------

## 🧱 Architecture Overview

``` text
Synthetic Data Generator (Python)
↓
Parquet Files (data/raw_y2)
↓
Cloudflare R2 Object Storage
↓
Snowflake RAW Schema (External Stage + COPY INTO)
↓
dbt Models
    ├─ Staging
    ├─ Foundation
    └─ Marts
↓
Airflow Orchestration
↓
Telegram Alerts
```

Each component represents a typical layer found in modern analytics
engineering pipelines.

------------------------------------------------------------------------

## ⚙️ Core Components

### 1️⃣ Data Generator

Location: 

    src/generator/runner_y2.py

This generator simulates SaaS operational activity including:

-   subscription lifecycle events
-   payment activity
-   product interaction events
-   user records

The generator can optionally **carry over users from the Year 1 dataset** to simulate a continuous SaaS lifecycle.

Output files are written as **Parquet datasets** to: 

    data/raw_y2/

------------------------------------------------------------------------

### 2️⃣ Object Storage Upload (Cloudflare R2)

Location: 

    src/utils/upload_to_r2.py

Responsibilities:

-   Upload all generated Parquet files to **Cloudflare R2**
-   Store data using the prefix: 

        y2/

R2 serves as the **cloud object storage layer** for ingestion into
Snowflake.

------------------------------------------------------------------------

### 3️⃣ Cloud-to-Cloud Ingestion (Snowflake)

Location: 

    src/ingestion/ingest_r2.py

This ingestion step loads data from **R2 object storage** into the
**Snowflake RAW schema**.

For each dataset (`subscription_events`, `payments`, `product_events`,
`users`), the script:

1.  Ensures the target RAW table exists
2.  Executes `COPY INTO` from the external stage
3.  Captures ingestion metadata

Metadata columns include:

-   `_stg_file_name`
-   `_stg_loaded_at`

These fields allow traceability of ingestion batches.

------------------------------------------------------------------------

### 4️⃣ Analytics Transformation (dbt)

Location: 

    dbt/

Data transformations follow a **layered analytics architecture**:

 | Layer       | Purpose |
 | ------------ | --------------------------------------------------- |
 | staging     | Data type normalization and field standardization |
 | foundation  | Core fact and dimension models |
 | marts       | Business-level analytical models |

dbt manages model dependencies, execution order, and data tests.

------------------------------------------------------------------------

### 5️⃣ Orchestration (Apache Airflow)

Location: 

    airflow/

The pipeline can be scheduled and monitored using **Apache Airflow**
running in Docker.

Responsibilities:

-   schedule pipeline runs
-   execute ingestion and dbt tasks
-   monitor task status
-   send alert notifications

Airflow services are containerized using **Docker Compose**.

------------------------------------------------------------------------

# 📊 Star Schema (Analytics Model)

The cloud stack uses a **star schema** modeled in dbt.  
The structure remains consistent with the local stack to maintain
**metric comparability between Year 1 and Year 2**, while adding extra
fields for monitoring and data quality.

```text
                 dim_date
                   |
                   |
dim_users ---- fct_payments

dim_users ---- fct_subscription_events ---- dim_plans
                   |
                   |
                dim_date

dim_users ---- fct_product_events ---- dim_plans
                   |
                   |
                dim_date
```

------------------------------------------------------------------------

## Dimension Tables

### dim_users

| Field | Description |
|------|------|
| user_key | Surrogate primary key |
| user_id | Natural key |
| name | User name |
| email | User email |
| country | User country |
| timezone | User timezone |
| acquisition_channel | Marketing acquisition source |

------------------------------------------------------------------------

### dim_date

| Field | Description |
|------|------|
| date_key | Surrogate key (YYYYMMDD) |
| full_date | Full calendar date |
| day | Day of month |
| day_name | Name of day |
| month | Month number |
| month_name | Month name |
| year | Calendar year |
| quarter | Quarter |
| is_weekend | Weekend indicator |
| batch_month | Batch control field used in Y2 processing |

------------------------------------------------------------------------

### dim_plans

| Field | Description |
|------|------|
| plan_key | Surrogate key |
| plan | Plan name |
| price_usd | Monthly price |
| effective_from | Plan start validity |
| effective_to | Plan end validity |
| is_current | Indicates active version |

Plan dimension supports plan evolution such as:

- Starter
- Growth
- Enterprise

------------------------------------------------------------------------

## Fact Tables

### fct_payments

**Grain:** 1 row = 1 payment attempt

Foreign keys:

- user_key
- date_key

Measures:

- amount_usd

Additional fields:

- status
- attempt_number
- is_late_arriving

------------------------------------------------------------------------

### fct_subscription_events

**Grain:** 1 row = 1 subscription lifecycle event

Foreign keys:

- user_key
- plan_key
- date_key

Attributes:

- event_type
- subscription_status
- referral_code
- is_late_arriving

------------------------------------------------------------------------

### fct_product_events

**Grain:** 1 row = 1 product usage event

Foreign keys:

- user_key
- plan_key
- date_key

Attributes:

- event_type
- referral_code
- event timestamps

------------------------------------------------------------------------

## 📂 Project Structure

``` text
02-cloud-stack/
│
├── README.md
│
├── src/
│   ├── generator/
│   ├── ingestion/
│   └── utils/
│
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── foundation/
│   │   └── marts/
│   ├── tests/
│   └── macros/
│
└── airflow/
    ├── docker-compose.yml
    ├── Dockerfile
    ├── Makefile
    └── dags/
```

This structure separates responsibilities between **data generation, ingestion, transformation, and orchestration**.

------------------------------------------------------------------------

## ⚙️ Environment Requirements

Ensure the following tools are available:

-   Python **3.10+**
-   Docker
-   Docker Compose
-   Snowflake account
-   Cloudflare R2 account
-   dbt Core
-   dbt Snowflake adapter (`dbt-snowflake`)

Install Python dependencies from the repository root:

``` bash
pip install -r requirements.txt
```

Note:

Additional dependencies (including dbt adapters) are installed inside
the **Airflow Docker image** defined in: 

    02-cloud-stack/airflow/Dockerfile

------------------------------------------------------------------------

## 🔐 Environment Configuration

Create a `.env` file at the repository root with the following
variables.

### Snowflake Configuration

``` env
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=...
```

### Cloudflare R2 Configuration

``` env
R2_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_BUCKET_NAME=...
```

### Telegram Alerts (Optional)

``` env
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

### Airflow Admin (Optional)

``` env
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

------------------------------------------------------------------------

## 🗄 Snowflake Objects Required

Before running ingestion, ensure the following objects exist in
Snowflake:

 | Object          | Name |
 | ---------------- | ----------------- |
 | Database        | `DATA_PLATFORM` |
 | Schema          | `RAW` |
 | File Format     | `fmt_parquet` |
 | External Stage  | `stg_r2_y2` |

The stage should point to the R2 bucket prefix: 

    y2/

The ingestion script uses the following constants:

    SNOWFLAKE_DB = DATA_PLATFORM
    SNOWFLAKE_SCHEMA = RAW
    STAGE_NAME = stg_r2_y2

------------------------------------------------------------------------

## ▶️ Running the Cloud Pipeline (Manual)

Navigate to the cloud stack directory:

``` bash
cd 02-cloud-stack
```

### Step 1 - Generate Year 2 Data

``` bash
python -m src.generator.runner_y2
```

Run without user carry-over:

``` bash
python -m src.generator.runner_y2 --no-carry-over
```

Generated files are written to: 

    data/raw_y2/

------------------------------------------------------------------------

### Step 2 - Upload Data to R2

``` bash
python -m src.utils.upload_to_r2
```

This uploads all `.parquet` files from `data/raw_y2` to the R2 bucket
using the prefix: 

    y2/

------------------------------------------------------------------------

### Step 3 - Ingest Data into Snowflake

``` bash
python -m src.ingestion.ingest_r2
```

This step loads data from the R2 external stage into the Snowflake `RAW`
schema.

------------------------------------------------------------------------

### Step 4 - Run dbt Transformations

``` bash
cd dbt
dbt debug
dbt run
dbt test
```

Run models by layer:

``` bash
dbt run --select staging
dbt test --select staging

dbt run --select foundation
dbt test --select foundation

dbt run --select marts
```

------------------------------------------------------------------------

## 🔄 Running the Pipeline with Airflow

Navigate to the Airflow directory:

``` bash
cd 02-cloud-stack/airflow
```

### First-Time Initialization

``` bash
make init
```

### Start Services

``` bash
make up
```

Airflow UI will be available at:

    http://localhost:8080

------------------------------------------------------------------------

## 📊 Main Airflow DAG

DAG Name: 

    saas_platform_pipeline

Pipeline tasks execute in the following order:

1.  `ingest_r2_to_snowflake`
2.  `dbt_run_staging`
3.  `dbt_test_staging` (non-blocking)
4.  `dbt_run_foundation`
5.  `dbt_test_foundation` (blocking)
6.  `dbt_run_marts`

Default schedule: 0 6 1 * *

Runs monthly on the **1st day at 06:00 UTC**.

------------------------------------------------------------------------

## 🔔 Pipeline Alerts

Alert notifications are sent via **Telegram**.

Events triggering alerts:

-   task failure → notification per task
-   DAG success → notification per pipeline run

------------------------------------------------------------------------

## 🎯 Design Philosophy

This cloud pipeline prioritizes:

-   clear separation of pipeline layers
-   deterministic transformations
-   observable ingestion processes
-   reproducible analytics workflows

The architecture reflects patterns commonly used in **modern analytics engineering platforms**.
