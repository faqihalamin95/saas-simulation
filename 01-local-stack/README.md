# SaaS Analytics Local Stack

## 📌 Overview

This module implements a **local end-to-end data pipeline** that
simulates SaaS subscription data and processes it through a layered
analytics architecture.

The pipeline demonstrates how raw event data can be generated, ingested
into a relational database, and transformed into analytics-ready models
using **dbt**.

The primary objective of this stack is to showcase **core data
engineering workflows**, including:

-   synthetic SaaS event data generation
-   raw data ingestion into PostgreSQL
-   layered analytics modeling (staging → foundation → marts)
-   reproducible local pipeline execution
-   SQL-based transformations with dbt

This environment represents a **local development stack**, typically
used during early development before moving to cloud infrastructure.

------------------------------------------------------------------------

## 🧱 Architecture Overview

    Synthetic Data Generator (Python)
            ↓
    Parquet Files (data/raw)
            ↓
    Ingestion Pipeline (Pandas + SQLAlchemy)
            ↓
    PostgreSQL Raw Schema
            ↓
    dbt Models
        ├─ Staging
        ├─ Foundation
        └─ Marts

Each stage has a clear responsibility and can be executed independently
using a `Makefile`.

------------------------------------------------------------------------

## 📊 Data Model (Star Schema)

The analytics layer in the **foundation** models follows a **star schema
design**.\
This structure simplifies analytical queries and supports dimensional
analysis across SaaS events.

The schema contains **three dimension tables** and **three fact
tables**.

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

This design enables flexible analysis across **users, time, and
subscription plans**.

------------------------------------------------------------------------

## Dimension Tables

### dim_users

Represents SaaS users and acquisition metadata.

Primary key: user_key

Natural key: user_id

Attributes:

-   name
-   email
-   country
-   timezone
-   acquisition_channel

------------------------------------------------------------------------

### dim_date

Provides a standard calendar dimension for time-based analytics.

Primary key: date_key

Format: YYYYMMDD (integer)

Attributes:

-   full_date
-   day
-   day_name
-   month
-   month_name
-   year
-   quarter
-   is_weekend

------------------------------------------------------------------------

### dim_plans

Represents SaaS subscription plans.

Primary key: plan_key

Attributes:

-   plan
-   price_usd
-   effective_from
-   effective_to
-   is_current

This table stores simple **historical plan changes**, similar to a
**slowly changing dimension approach**.

------------------------------------------------------------------------

## Fact Tables and Grain

Each fact table has a clearly defined **grain** to avoid ambiguity in
analytics.

------------------------------------------------------------------------

### fct_payments

Grain: 1 row = 1 payment attempt

Foreign keys:

-   user_key
-   date_key

Measures:

-   amount_usd

Event attributes:

-   status
-   attempt_number
-   event_timestamp_utc
-   event_timestamp_local

This table captures **all payment attempts**, including failures and
retries.

------------------------------------------------------------------------

### fct_subscription_events

Grain: 1 row = 1 subscription lifecycle event

Foreign keys:

-   user_key
-   plan_key
-   date_key

Attributes:

-   event_type
-   subscription_status
-   event_timestamp_utc
-   event_timestamp_local

Examples of events:

-   trial_started
-   subscribed
-   upgraded
-   downgraded
-   cancelled

------------------------------------------------------------------------

### fct_product_events

Grain: 1 row = 1 product usage event

Foreign keys:

-   user_key
-   plan_key
-   date_key

Attributes:

-   event_type
-   event_timestamp_utc
-   event_timestamp_local

This table captures product interactions such as:

-   feature usage
-   login events
-   activity tracking

------------------------------------------------------------------------

## ⚙️ Core Components

### 1️⃣ Data Generator

Location: src/generator/

Simulates SaaS business activity including:

-   user lifecycle events
-   subscription and payment activity
-   product and plan usage

Generated datasets are stored as **Parquet files** in: data/raw/

------------------------------------------------------------------------

### 2️⃣ Data Ingestion

Location: src/ingestion/ingest.py

Responsibilities:

-   Load Parquet files into PostgreSQL
-   Write data into the `raw` schema
-   Perform basic schema handling
-   Support simple schema evolution when new fields appear

Technology used:

-   Pandas
-   SQLAlchemy

------------------------------------------------------------------------

### 3️⃣ Database Initialization

Location: src/utils/init_db.py

Purpose:

-   Reset the database schema before ingestion
-   Ensure a clean and reproducible environment

This step **drops and recreates the `raw` schema** before loading data.

------------------------------------------------------------------------

### 4️⃣ Analytics Transformation (dbt)

Location: dbt/

The transformation layer follows a **layered analytics architecture**:

-   **staging** → data type normalization and column standardization
-   **foundation** → core fact and dimension models
-   **marts** → business-level metrics and analytical outputs

dbt manages SQL transformations, dependencies, and testing.

------------------------------------------------------------------------

## 📂 Project Structure

    01-local-stack/
    │
    ├── Makefile
    ├── README.md
    │
    ├── data/
    │   └── raw/
    │
    ├── src/
    │   ├── generator/
    │   ├── ingestion/
    │   └── utils/
    │       ├── db.py
    │       └── init_db.py
    │
    └── dbt/
        ├── dbt_project.yml
        └── models/
            ├── staging/
            ├── foundation/
            └── marts/

------------------------------------------------------------------------

## ⚙️ Environment Requirements

Ensure the following tools are installed locally:

-   Python **3.10+**
-   PostgreSQL (**recommended v16**)
-   `pip`
-   **dbt Core**
-   **dbt-postgres adapter**

Install Python dependencies from the repository root:

``` bash
pip install -r requirements.txt
```

Note: `dbt` is not included in `requirements.txt` and must be installed
manually.

------------------------------------------------------------------------

## 🔐 Environment Configuration

Database connections are configured using environment variables read by: src/utils/db.py

Example `.env` file:

    DB_USER=postgres
    DB_PASSWORD=postgres
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=saas_sim

------------------------------------------------------------------------

## ▶️ Running the Local Pipeline

Navigate to the project: cd 01-local-stack

### Run the Full Pipeline

    make all

This runs:

    generate → init → ingest → test

------------------------------------------------------------------------

### Run Steps Individually

    make generate
    make init
    make ingest
    make test

  Command         Description
  --------------- ------------------------------------
  make generate   Generate synthetic SaaS data
  make init       Reset raw database schema
  make ingest     Load Parquet files into PostgreSQL
  make test       Run validation checks

------------------------------------------------------------------------

## 🔄 Running dbt Transformations

Navigate to the dbt project: cd 01-local-stack/dbt

Run:

    dbt debug
    dbt run
    dbt test

Model layers:

  Layer        Purpose
  ------------ ----------------------------------
  staging      Standardizes raw data
  foundation   Builds fact and dimension tables
  marts        Produces business metrics

Examples of analytics outputs:

-   MRR movement
-   customer retention
-   lifetime value

------------------------------------------------------------------------

## 🎯 Purpose of This Stack

This local stack supports:

-   rapid experimentation
-   deterministic pipeline development
-   reproducible analytics modeling

The design intentionally favors **clarity and simplicity** over complex
orchestration or infrastructure.
