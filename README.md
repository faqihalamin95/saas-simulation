# SaaS Analytics Pipeline

A comprehensive analytics engineering project simulating real-world SaaS subscription data with intentional data quality challenges, built incrementally from local development to cloud production architecture.

---

## 🎯 Overview

This project implements an end-to-end SaaS analytics pipeline built on synthetically generated subscription data.

Unlike typical portfolio projects that rely on pre-cleaned public datasets, this pipeline generates its own raw data, intentionally injects data quality issues, and processes them through a layered and reproducible architecture.

### The primary objective of this project is to demonstrate:

- deterministic pipeline design
- subscription-based dimensional modeling
- temporal & cohort analysis
- Slowly Changing Dimensions (SCD Type 2)
- data quality handling under schema instability
- incremental-to-scalable architectural thinking
- business-metric-driven transformation (MRR, churn, retention, LTV)

> **This is not a dashboard-first project.** 
> **This is an analytics engineering & pipeline design project.**

---

## 🧱 Architecture Overview

### The pipeline follows a layered architecture pattern:

```text
Synthetic Data Generator
↓
Raw Layer
↓
Staging Layer
↓
Foundation Layer
↓
Marts Layer
```

Each layer has explicit responsibility boundaries.

### The project is implemented in two architectural phases:

- **Phase 1 — Local Stack (Year 1)**

- **Phase 2 — Cloud Stack (Year 2)**

The second phase is intentionally dependent on business validation from the first phase.

---

## 🏗️ Architectural Philosophy

### This project adopts a deliberate scaling strategy:

```text
Start local → validate business → scale infrastructure.
```

Infrastructure investment is treated as **a consequence of validated unit economics**, not **a starting assumption**.

### This mirrors real startup evolution:

- validate with minimal infrastructure
- only scale once justified by data

---

## 🎲 Data Generation Strategy

Instead of consuming external CSV data, this project builds **a synthetic SaaS simulator**.

### Generator Responsibilities

- User lifecycle simulation:

    Trial → Conversion → Upgrade/Downgrade → Churn → Reactivation

- Payment simulation:

    - Success/failure logic
    - Retry rules
    - 3-strike cancellation policy

- Product usage event tracking
- Multi-timezone handling (7 countries)

### Weighted probabilities simulate realistic SaaS behavior:

- Trial conversion ≈ 40%
- Monthly churn ≈ 12%
- Seasonal Q4 slowdown
- Multiple acquisition channels

The generator produces realistic but controlled complexity.

---

## ⚠️ Chaos Engineering (Data Quality Simulation)

**This project intentionally injects data quality issues to simulate real-world instability.**

### Scenario:
- Late-arriving events
- Schema evolution, because new columns added mid-year	
- Duplicate payments	
- Type drift: Numeric → String	
- Plan rename, from business rebrand	

Not all issues are "fixed".

Some are:
- documented
- tracked
- made observable

The goal is pipeline resilience.

---

## 📊 Project Structure

```text
.
├── README.md                          
├── 01-local-stack/                    # Year 1 implementation
│   ├── README.md                      # Local stack details
│   ├── src/
│   │   ├── generator/                 # Synthetic data simulator
│   │   ├── ingestion/
│   │   └── utils/
│   ├── dbt/                           # dbt project
│   │   ├── models/
│   │   │   ├── staging/               # Clean & cast raw data
│   │   │   ├── foundation/            # Star schema (dims + facts)
│   │   │   └── marts/                 # Business metrics
│   │   ├── seeds/                     # Static reference data
│   │   └── dbt_project.yml
│   ├── data/
│   │   ├── raw/                       # Generated parquet files
│   │   └── reports/                   # Validation outputs
│   └── Makefile                       # Task orchestration
│   
│
├── 02-cloud-stack/                    # Year 2 
└── requirements.txt
```

---

## 🛠️ Tech Stack 

### Phase 1 – Local

- Python (data generation)
- Pandas
- PostgreSQL 16
- dbt Core
- Makefile (local orchestration)
- DBeaver (inspection)

The stack is intentionally simple and reproducible.

### Phase 2 – Cloud Evolution

Triggered only if:

Year 1 demonstrates sustainable unit economics.

Planned enhancements:

- Apache Airflow orchestration
- Snowflake
- dbt Cloud
- Real-time dashboards
- CI/CD integration

**10× data volume simulation**
Business validation precedes infrastructure scaling.

---

## 📈 Core Business Question

**The entire pipeline exists to answer:**

- Is this SaaS business sustainable for scaling?

All marts and transformations are derived from this question.

This prevents scope creep and metric inflation.

---

## 🎓 What This Project Demonstrates

- **Subscription-based dimensional modeling**
- **SCD Type 2 implementation**
- **Temporal revenue attribution**
- **Chaos-aware pipeline design**
- **Business-driven analytics engineering**
- **Incremental architecture thinking (local → cloud)**

---

## 🔮 Design Philosophy

### This project prioritizes:

- Explicit modeling over implicit assumptions
- Deterministic builds over uncontrolled incrementals
- Business-first metrics over vanity dashboards
- Simulated realism over sanitized datasets
- Architectural evolution over premature cloud complexity

Complexity is introduced intentionally — not accidentally.

---

## 📌 Notes

This project is designed as an advanced analytics engineering portfolio project demonstrating subscription analytics modeling under real-world instability conditions.

**It is intentionally built in stages to reflect how real systems evolve.**