# SaaS Analytics Pipeline

This project is an end-to-end **analytics engineering simulation for a SaaS subscription business**, built incrementally by phase.

The primary focus is not dashboarding, but a **robust data pipeline design** from generation, ingestion, and transformation to data quality control and orchestration.

---

## 🎯 Project Goals

This project demonstrates:

- deterministic and reproducible pipeline design,
- subscription-based dimensional modeling,
- MRR/churn/retention/LTV analysis,
- handling data quality issues (schema drift, late-arriving data, duplicates),
- gradual scaling strategy from local to cloud,
- readiness for a downstream analytics consumption/BI phase.

---

## 🧱 Data Architecture (Common Pattern)

All phases follow the same layered pattern:

```text
Synthetic Data Generator
        ↓
Raw Layer
        ↓
Staging Layer
        ↓
Foundation Layer (Star Schema)
        ↓
Marts Layer (Business Metrics)
```

---

## 🚀 Project Phases

### Phase 1 — Local Stack (Year 1)

The goal of this phase is to validate business metrics and data modeling with minimal infrastructure.

- Generate local synthetic data (Parquet)
- Ingest into PostgreSQL
- Transform with dbt (staging → foundation → marts)
- Run basic pipeline validation

📘 Setup and runbook:
- [`01-local-stack/README.md`](01-local-stack/README.md)

---

### Phase 2 — Cloud Stack (Year 2)

This phase scales the architecture to a cloud-like setup after metric and model validation.

- Upload Y2 data to Cloudflare R2
- Ingest R2 → Snowflake RAW
- Run dbt transformations in Snowflake
- Schedule monthly orchestration with Airflow
- Use Telegram alerts for operational observability

📘 Setup and runbook:
- [`02-cloud-stack/README.md`](02-cloud-stack/README.md)

---

### Phase 3 — Analytics Consumption & Decision Layer (Project Direction)

Phase 3 is the forward direction of this project: turning mart outputs into a business decision consumption layer.

Target scope for Phase 3:

- consistent semantic/business metric layer across stakeholders,
- dashboards and monitoring for core KPIs (MRR movement, retention, LTV, data quality),
- threshold-based business alerting,
- lightweight governance for metric definitions and quality SLAs,
- readiness for CI/CD analytics workflows.

> Note: The repository currently provides implemented foundations for Phases 1 and 2. Phase 3 is the natural continuation on top of existing marts.

---

## 📊 Repository Structure

```text
.
├── README.md
├── requirements.txt
├── 01-local-stack/
│   ├── README.md
│   ├── Makefile
│   ├── src/
│   └── dbt/
└── 02-cloud-stack/
    ├── README.md
    ├── src/
    ├── dbt/
    └── airflow/
```

---

## 🛠️ Tech Stack by Phase

### Phase 1 (Local)
- Python, Pandas, PyArrow
- PostgreSQL
- dbt Core
- Makefile

### Phase 2 (Cloud)
- Cloudflare R2 (object storage)
- Snowflake (raw + transformation target)
- dbt + custom tests
- Apache Airflow (Dockerized orchestration)
- Telegram (operational alerts)

---

## 🧠 Core Business Question

All transformations are designed to answer one central question:

**Is this SaaS business healthy and ready to scale?**

As a result, marts and quality checks prioritize metrics that directly affect business decisions.

---

## 🔮 Design Principles

- Start local, validate value, then scale infrastructure.
- Business metrics first, tools second.
- Observability and quality are core features, not add-ons.
- Evolve architecture incrementally, not through big-bang migration.