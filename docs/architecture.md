# Architecture — Supply Chain LDIP

## Overview

The Late-Delivery Intelligence Platform (LDIP) is a layered, orchestrated analytics pipeline that ingests raw supply chain order data, validates and models it dimensionally, and surfaces fulfillment risk to BI tools before SLA breach.

## Pipeline Layers

```
[DataCo CSV — 180K rows]
        │
        ▼ (Airflow FileSensor)
┌─────────────────────────────────────────────────────┐
│  raw schema  (Bronze)                               │
│  raw.orders — all 53 source columns as VARCHAR      │
│  raw.load_log — batch metadata + idempotency guard  │
└─────────────────────────────────────────────────────┘
        │
        ▼ (Python: dedup + type casting + DQ checks)
┌─────────────────────────────────────────────────────┐
│  staging schema  (Silver)                           │
│  stg_orders       — typed orders (order_id grain)   │
│  stg_order_items  — typed line items                │
│  stg_customers    — extracted customer entities     │
│  stg_products     — extracted product entities      │
│  stg_suppliers    — department-as-supplier          │
│  dq_results       — DQ check outcomes               │
└─────────────────────────────────────────────────────┘
        │
        ▼ (Python: SCD2 merge + surrogate keys + derived fields)
┌─────────────────────────────────────────────────────┐
│  warehouse schema  (Gold — Star Schema)             │
│  Dimensions:                                        │
│    dim_date          — pre-generated calendar       │
│    dim_shipping_mode — 4 modes reference table      │
│    dim_geography     — market/region/country/city   │
│    dim_customer      — SCD Type 2 (segment/address) │
│    dim_product       — SCD Type 1                   │
│    dim_supplier      — SCD Type 2 (performance tier)│
│  Facts (partitioned by year):                       │
│    fct_orders        — order grain                  │
│    fct_order_items   — order-item grain             │
│    fct_shipments     — shipment event grain         │
└─────────────────────────────────────────────────────┘
        │
        ▼ (SQL aggregations — Airflow mart tasks)
┌─────────────────────────────────────────────────────┐
│  marts schema  (Aggregated)                         │
│  mart_supplier_performance   — monthly by supplier  │
│  mart_route_efficiency       — mode × region × month│
│  mart_customer_segment_value — RFM by customer/month│
│  mart_inventory_velocity     — turnover by product  │
│  mart_late_delivery_risk     — ML risk scores       │
└─────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────┐    ┌──────────────────┐
│  Tableau Public  │    │  Power BI Desktop │
└──────────────────┘    └──────────────────┘
```

## Orchestration (Airflow)

All data movement is controlled by Airflow 2.9 (LocalExecutor).

**`dag_supply_chain_daily`** — runs at 06:00 UTC daily:
1. `FileSensor` — watches `data/raw/` for new CSV drop
2. `load_raw` — raw CSV → `raw.orders`
3. `run_dq_checks` — YAML-driven checks → `staging.dq_results`
4. `branch_on_dq` — critical failures skip warehouse/mart tasks
5. `stage_orders` — raw → `staging.stg_*`
6. `load_warehouse` — staging → `warehouse.*` (SCD2 merges)
7. `load_marts` — warehouse → `marts.*`
8. `run_ml_predictions` — sklearn classifier → `mart_late_delivery_risk`
9. `notify` — logs run summary to `metadata.pipeline_runs`

**`dag_supply_chain_backfill`** — parameterized date range, re-processes historical windows.

## SCD Type 2 Strategy

`dim_customer` and `dim_supplier` use SCD2 to track changes over time:

- A `row_hash` (MD5 of tracked attributes) is computed each run.
- On hash mismatch: close the current version (`effective_to = today - 1`, `is_current = 0`) and insert a new version.
- On hash match: no-op (idempotent).
- Facts always join to the dimension version active on the order date via `effective_from ≤ order_date < effective_to`.

## Partitioning

`fct_orders`, `fct_order_items`, and `fct_shipments` are range-partitioned by `order_date_key` (YYYYMMDD) into annual partitions. MySQL does not support FK constraints on partitioned tables; referential integrity is enforced at the ETL layer.

## Tech Stack

| Layer | Tool |
|---|---|
| Storage | MySQL 8.0 (Docker) |
| Transformation | Python 3.11 (Pandas, NumPy) |
| Orchestration | Apache Airflow 2.9 (LocalExecutor) |
| ML | scikit-learn |
| BI | Tableau Public + Power BI Desktop |
| CI/CD | GitHub Actions |
| Containers | Docker Compose |
