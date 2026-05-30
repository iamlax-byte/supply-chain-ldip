# Supply Chain Late-Delivery Intelligence Platform (LDIP)

End-to-end data engineering portfolio project: ingests 180K supply chain order events daily, models them in a MySQL star schema, orchestrates with Airflow, and surfaces late-delivery risk to Tableau and Power BI.

> **Portfolio framing:** Designed for a senior data engineer / analytics engineer role. Every design decision reflects production discipline — SCD2 on two dimensions, YAML-driven DQ framework, partitioned fact tables, idempotent MERGE patterns, and CI on push.

---

## Architecture

```
[DataCo CSV — Kaggle]
       │ FileSensor
       ▼
  raw.orders            ← Bronze: as-ingested, all columns as VARCHAR
       │ DQ checks + type casting
       ▼
  staging.stg_*         ← Silver: typed, deduped, validated
       │ SCD2 merge + derived fields
       ▼
  warehouse.*           ← Gold: star schema (3 facts, 6 dims)
       │ SQL aggregations
       ▼
  marts.*               ← Business-ready aggregated views
       │
  Tableau Public + Power BI Desktop
```

See [`docs/architecture.md`](docs/architecture.md) for the full Airflow DAG design, SCD2 strategy, and partitioning rationale.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Storage | MySQL 8.0 (Docker) |
| Transform | Python 3.11 · Pandas · NumPy |
| Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| ML | scikit-learn |
| BI | Tableau Public · Power BI Desktop |
| CI/CD | GitHub Actions |
| Containers | Docker Compose |

**Cost: $0.** All tools are free tier or open source.

---

## Quick Start

### 1. Prerequisites
- Docker Desktop (running)
- Python 3.11+
- Kaggle account (for dataset download)

### 2. Clone and configure
```bash
git clone https://github.com/iamlax-byte/supply-chain-ldip.git
cd supply-chain-ldip
cp .env.example .env
# Edit .env — set strong passwords and generate a Fernet key
```

### 3. Download the dataset
```bash
bash scripts/download_data.sh
# Dataset lands at: data/raw/DataCoSupplyChainDataset.csv
```

### 4. Start the Docker stack
```bash
docker-compose build                    # build custom Airflow image (~3 min first time)
docker-compose up ldip-airflow-init     # one-time DB migration
docker-compose up -d                    # start all services
```

Services:
- Airflow UI: http://localhost:8080 (admin / admin)
- MySQL: localhost:3306

### 5. Trigger the pipeline
```bash
# Option A: Airflow UI → dag_supply_chain_daily → Trigger DAG
# Option B: CLI
docker exec ldip-airflow-scheduler airflow dags trigger dag_supply_chain_daily
```

### 6. Inspect results
```bash
docker exec -it ldip-mysql mysql -u ldip_user -p
```

---

## Build Phases

| Phase | Status | Description |
|---|---|---|
| 1 — Data Foundation | ✅ Complete | Docker stack, all DDL, raw loader |
| 2 — Python Transforms | 🔲 Next | Dedup, DQ framework, SCD2, derived fields |
| 3 — Airflow DAGs | 🔲 Planned | Daily + backfill DAGs, sensors, branching |
| 4 — Analytics Layer | 🔲 Planned | 5 mart SQL views, Tableau + Power BI |
| 5 — Senior Polish | 🔲 Planned | ML classifier, CI, Mermaid diagram |

---

## Data Model

**Facts:** `fct_orders` · `fct_order_items` · `fct_shipments`

**Dimensions:** `dim_customer` (SCD2) · `dim_supplier` (SCD2) · `dim_product` (SCD1) · `dim_geography` · `dim_date` · `dim_shipping_mode`

**Marts:** `mart_supplier_performance` · `mart_route_efficiency` · `mart_customer_segment_value` · `mart_inventory_velocity` · `mart_late_delivery_risk`

See [`docs/data-dictionary.md`](docs/data-dictionary.md) for column-level documentation.

---

## Dataset

**DataCo Smart Supply Chain** — Kaggle  
~180K rows · 53 columns · Covers orders, shipments, customers, products, 2015–2018.

Place at: `data/raw/DataCoSupplyChainDataset.csv` (gitignored — too large to commit)

---

## Non-Negotiables (Senior-Level Design)

1. **Idempotent loads** — re-running yesterday's DAG produces the same warehouse state
2. **SCD Type 2** on customer + supplier — historical tracking, not current-state-only
3. **YAML-driven DQ** — 20+ validation rules in `config/dq_rules.yaml`, not hardcoded asserts
4. **Layered architecture** — raw → staging → warehouse → marts; no layer skipping
5. **Backfill DAG** — parameterized, re-processes any date window cleanly
6. **Metadata observability** — every run logs row counts + DQ pass rate to `metadata.pipeline_runs`
7. **Unit tests on transformations** — pytest, minimum 70% coverage enforced by CI
8. **CI on push** — lint (ruff + black) + tests + DAG validation via GitHub Actions
