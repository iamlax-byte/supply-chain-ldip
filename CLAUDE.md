# Supply Chain Late-Delivery Intelligence Platform (LDIP)

> An end-to-end data engineering project: ingests multi-source supply chain data, models it dimensionally on MySQL, transforms with Python, orchestrates with Airflow, and surfaces fulfillment risk to Tableau and Power BI.

---

## Mission

Build a production-grade analytics platform that detects late-delivery risk before SLA breach, tracks supplier degradation over time, and surfaces route inefficiencies — owning every layer from raw ingestion to BI consumption.

This is a portfolio project for a senior data engineer / analytics engineer role. Every design decision should reflect senior-level production discipline, not tutorial-level convenience.

---

## Problem Statement

Mid-to-large supply chain operators run fulfillment on fragmented systems: orders in ERP, shipments in TMS, suppliers in procurement, customers in CRM. Each system reports its own truth on its own cadence. Consequences:

- Late deliveries detected **after** SLA breach
- Supplier degradation surfaces only at quarterly reviews
- Route inefficiencies persist because no unified view exists across shipping mode × region

For an operator processing 5,000+ orders/day at a 5% late rate, this translates to six-figure monthly margin bleed.

**The gap this platform closes:** a reliable daily pipeline that ingests, validates, dimensionally models, historically tracks (SCD2), and surfaces fulfillment risk to operators *before* breach — not after.

---

## Tech Stack & Rationale

| Layer | Tool | Why |
|---|---|---|
| Storage | **MySQL 8** (Docker) | Mirrors mid-market reality where teams don't have Snowflake; forces deliberate indexing, partitioning, merge strategy |
| Transformation | **Python 3.11** (Pandas/NumPy) | Portable, testable, version-controlled logic; lingua franca every interviewer reads |
| Orchestration | **Airflow 2.9+** (LocalExecutor) | File sensors, SLAs, retries, dynamic mapping, auditable DAG when things break at 3am |
| ML | **scikit-learn** | Late-delivery classifier integrated into pipeline |
| BI | **Tableau Public + Power BI Desktop** | Tableau for shareable public link; Power BI for enterprise demoability |
| CI/CD | **GitHub Actions** | Lint, unit tests, DAG validation on push |
| Containers | **Docker Compose** | Reproducible local stack (MySQL + Airflow) |

**Cost: $0.** All tools are free tier or open source.

---

## Architecture

```
[Raw CSV: DataCo Smart Supply Chain — Kaggle]
          ↓ (file sensor)
[MySQL: raw_layer]            ← Bronze: as-ingested, with batch_id + ingestion_ts
          ↓ (Pandas validation + dedup + DQ checks)
[MySQL: staging]              ← Silver: cleaned, typed, deduped
          ↓ (dimensional transforms + SCD2)
[MySQL: warehouse]            ← Gold: star schema, conformed dims
          ↓ (SQL aggregations)
[MySQL: marts]                ← Aggregated business views
          ↓
[Tableau Public + Power BI]
          ↑
[Airflow] orchestrates entire flow — sensors, SLAs, retries, branching, alerts
```

---

## Data Model (Star Schema)

**Fact tables:**
- `fct_orders` — grain: one row per order
- `fct_order_items` — grain: one row per order line
- `fct_shipments` — grain: one row per shipment event

**Dimension tables:**
- `dim_customer` — **SCD Type 2** (track segment/address changes over time)
- `dim_product` — SCD1
- `dim_supplier` — **SCD Type 2** (track performance tier changes)
- `dim_geography` — region/country/city hierarchy
- `dim_date` — pre-generated calendar with fiscal attributes
- `dim_shipping_mode` — standard/first/second class/same day

**Derived fields in warehouse:**
- `delivery_delay_days` = actual_delivery - scheduled_delivery
- `is_late_flag` = boolean
- `profit_margin_pct`
- `supplier_score` = weighted composite (on-time rate × quality × cost)

**Marts:**
- `mart_supplier_performance` — on-time rate, defect rate, cost per supplier per month
- `mart_route_efficiency` — shipping mode × region × avg delay
- `mart_customer_segment_value` — RFM scoring
- `mart_inventory_velocity` — turnover by category
- `mart_late_delivery_risk` — predictions from ML model joined to live orders

---

## Build Phases

### Phase 1 — Data Foundation (Week 1)
- [x] Docker Compose: MySQL 8 + Airflow stack
- [x] Three schemas: `raw`, `staging`, `warehouse`, plus `marts`
- [x] DDL with indexes, FK constraints, partitioning where appropriate
- [x] Load DataCo CSV into raw with `load_batch_id`, `ingestion_ts`

### Phase 2 — Python Transformations (Week 2)
- [ ] Dedup module (composite key strategy)
- [ ] DQ framework: YAML-driven rules → results logged to `dq_results` table
- [ ] SCD2 merge logic for customer + supplier dims
- [ ] Date dimension generator
- [ ] Derived fields module (delay, margin, supplier score)
- [ ] Unit tests for every transformation function

### Phase 3 — Airflow Orchestration (Week 3)
- [ ] `dag_supply_chain_daily`: sensor → raw load → DQ → staging → warehouse → marts → notify
- [ ] `dag_supply_chain_backfill`: parameterized date range
- [ ] Branching on DQ failure (skip downstream)
- [ ] SLA monitoring, retries with exponential backoff
- [ ] XComs passing batch metadata between tasks
- [ ] Dynamic task mapping over regions
- [ ] Metadata table: `pipeline_runs` with row counts, runtime, DQ pass rate

### Phase 4 — Analytics Layer (Week 4)
- [ ] Build 5 mart SQL views
- [ ] Tableau Public dashboard: supplier scorecard + late delivery heatmap
- [ ] Power BI Desktop dashboard: executive KPIs + drill-through to order detail
- [ ] Screenshots in README

### Phase 5 — Senior-Level Polish (Week 5)
- [ ] Late-delivery classifier (sklearn), predictions written to `mart_late_delivery_risk`
- [ ] GitHub Actions: pytest, ruff/black lint, DAG validation
- [ ] Mermaid architecture diagram in README
- [ ] Data dictionary as Markdown
- [ ] README with the problem narrative, architecture, screenshots, run instructions

---

## File / Folder Structure

```
supply-chain-ldip/
├── CLAUDE.md                  # this file
├── README.md
├── docker-compose.yml
├── Dockerfile.airflow
├── requirements.txt
├── .gitignore
├── .github/workflows/ci.yml
├── config/
│   ├── dq_rules.yaml
│   └── pipeline_config.yaml
├── data/
│   ├── raw/                   # gitignored
│   └── samples/               # small samples committed for demo
├── sql/
│   ├── ddl/
│   ├── marts/
│   └── views/
├── src/
│   ├── ingestion/
│   ├── transformations/
│   ├── quality/
│   ├── ml/
│   └── utils/
├── airflow/dags/
├── tests/
│   ├── unit/
│   └── integration/
├── docs/
│   ├── architecture.md
│   ├── data-dictionary.md
│   └── diagrams/
└── dashboards/
    ├── tableau/               # .twbx + screenshots
    └── powerbi/               # .pbix + screenshots
```

---

## Coding Conventions

- **Python:** 3.11, type hints on every function signature, docstrings on every module
- **Style:** `ruff` + `black` (line length 100)
- **SQL:** lowercase keywords, snake_case identifiers, CTEs over subqueries
- **Naming:**
  - Facts: `fct_<grain>`
  - Dimensions: `dim_<entity>`
  - Staging: `stg_<source>_<entity>`
  - Marts: `mart_<business_concept>`
- **Idempotency:** every load is re-runnable; use MERGE patterns with batch IDs
- **Logging:** structured (JSON), every transformation logs row count in + row count out + duration
- **Testing:** every transformation function has a pytest unit test with a sample DataFrame fixture
- **Secrets:** `.env` file (gitignored); never hardcode credentials

---

## Non-Negotiables

These are the design principles that make the project look senior. Do not skip them for convenience:

1. **Idempotent loads.** Re-running yesterday's DAG produces the same warehouse state.
2. **SCD Type 2 on customer + supplier.** No "current state only" shortcut.
3. **DQ framework as config.** Rules live in `dq_rules.yaml`, not hardcoded asserts.
4. **Layered architecture.** Raw → staging → warehouse → marts. No skipping layers.
5. **Backfill DAG.** Must exist and must be parameterized.
6. **Metadata observability.** Every run logs row counts, runtime, DQ results to a metadata table.
7. **Unit tests on transformations.** Not optional.
8. **CI on push.** Lint + tests + DAG validation must pass.

---

## How to Work With Me (Instructions for Claude Code)

- **Always read `docs/architecture.md` and the relevant SQL DDL before designing new tables or transformations.** Stay consistent with the established model.
- **Propose before generating large files.** For anything over ~100 lines, describe the structure first, get my approval, then write.
- **Explain SCD2, merge logic, and DAG dependency choices verbosely in code comments** — I need to be able to explain every line in an interview.
- **Prefer clarity over cleverness.** This code is read by interviewers, not just executed.
- **When in doubt, ask.** If a design decision has tradeoffs (e.g. natural keys vs surrogate keys, MERGE vs INSERT+UPDATE), surface them and let me pick.
- **Update `docs/` as you go.** Architecture changes go in `architecture.md`, schema changes in `data-dictionary.md`.
- **Commit messages should be descriptive.** Format: `<type>(<scope>): <description>` — e.g. `feat(scd2): implement customer dimension type 2 merge`.
- **Never blanket-approve destructive commands.** Surface drops, truncates, deletes for explicit confirmation.

---

## Dataset

**Primary:** DataCo Smart Supply Chain (Kaggle) — ~180K rows, includes `Late_delivery_risk` flag and actual delivery dates. Search "DataCo Smart Supply Chain" on Kaggle.

**Place at:** `data/raw/DataCoSupplyChainDataset.csv`

**Download:** `bash scripts/download_data.sh`

---

## Success Criteria

This project is "done" when:

- [ ] The full DAG runs end-to-end without manual intervention
- [ ] Backfill DAG re-processes a 30-day window cleanly
- [ ] All DQ rules execute and log to metadata
- [ ] Two BI dashboards (Tableau + Power BI) are live with screenshots in README
- [ ] ML model writes predictions to `mart_late_delivery_risk`
- [ ] CI passes on `main`
- [ ] README tells the full story: problem → architecture → results
- [ ] I can whiteboard every design decision in an interview

---

## Quick Start (for future me)

```bash
# 1. Configure environment
cp .env.example .env   # fill in passwords

# 2. Download the dataset
bash scripts/download_data.sh

# 3. Build and start the stack
docker-compose build
docker-compose up ldip-airflow-init   # one-time init
docker-compose up -d

# 4. Trigger the DAG
# Open http://localhost:8080 → dag_supply_chain_daily → Trigger

# 5. Inspect results
docker exec -it ldip-mysql mysql -u ldip_user -p
```
