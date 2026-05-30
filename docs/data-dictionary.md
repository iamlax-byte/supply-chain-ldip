# Data Dictionary — Supply Chain LDIP

## Source: DataCo Smart Supply Chain (Kaggle)

~180,000 rows · 53 columns · Encoding: ISO-8859-1

| Source Column | Raw Column | Type (Staging) | Description |
|---|---|---|---|
| Type | type | varchar | Order type (e.g. DEBIT, TRANSFER) |
| Days for shipping (real) | days_for_shipping_real | tinyint | Actual transit days |
| Days for shipment (scheduled) | days_for_shipment_scheduled | tinyint | Committed SLA days |
| Benefit per order | benefit_per_order | decimal(12,2) | Gross profit per order |
| Sales per customer | sales_per_customer | decimal(12,2) | Revenue attributed to customer |
| Delivery Status | delivery_status | varchar | Advance / Late / On-time / Canceled |
| Late_delivery_risk | late_delivery_risk | tinyint(1) | 1 = at-risk flag from source |
| Category Id | category_id | int | Product category identifier |
| Category Name | category_name | varchar | Product category label |
| Customer Id | customer_id | int | Customer business key |
| Customer Segment | customer_segment | varchar | Consumer / Corporate / Home Office |
| Department Id | department_id | int | Supplying department → maps to supplier_id |
| Department Name | department_name | varchar | Supplying department name → supplier_name |
| order date (DateOrders) | order_date | datetime | Order placement timestamp |
| Order Id | order_id | int | Order business key |
| Order Item Id | order_item_id | int | Line item business key |
| Order Item Quantity | order_item_quantity | int | Units ordered |
| Sales | sales | decimal(12,2) | Line item revenue |
| Shipping Mode | shipping_mode | varchar | Standard / First / Second / Same Day |
| shipping date (DateOrders) | shipping_date | datetime | Actual ship timestamp |
| Product Card Id | product_card_id | int | Product business key |

*(Full 53-column mapping is in `src/ingestion/load_raw.py → COLUMN_MAP`)*

---

## raw Schema (Bronze)

### raw.orders
| Column | Type | Notes |
|---|---|---|
| id | bigint PK | Auto-increment surrogate |
| [53 source columns] | varchar | All as strings, no coercion |
| load_batch_id | varchar(50) | Batch run identifier |
| source_file | varchar(500) | Source CSV filename |
| ingestion_ts | timestamp | Load timestamp (MySQL default) |

### raw.load_log
| Column | Type | Notes |
|---|---|---|
| batch_id | varchar(50) | Links rows to a load event |
| source_file | varchar(500) | Filename as idempotency key |
| rows_loaded | bigint | Total rows written in this batch |
| status | varchar(20) | SUCCESS / FAILED |

---

## staging Schema (Silver)

### staging.stg_orders  *(order_id grain)*
Key derived fields:
- `delivery_delay_days` = `days_for_shipping_real` − `days_for_shipment_scheduled`
- `is_late_flag` = 1 when `delivery_delay_days > 0`
- `profit_margin_pct` = `order_profit / sales_amount`

### staging.stg_customers
- `row_hash` (MD5) covers: `customer_segment, customer_city, customer_state, customer_country, customer_zipcode, customer_street`
- Hash change triggers SCD2 new version in `warehouse.dim_customer`

### staging.stg_suppliers
- Natural key: `supplier_id` = `department_id` from source
- `performance_tier` assigned by composite_score thresholds:
  - ≥ 80 → platinum
  - ≥ 65 → gold
  - ≥ 50 → silver
  - < 50 → bronze

---

## warehouse Schema (Gold)

### Dimension Keys
All warehouse surrogate keys are `bigint / int auto_increment`. Natural/business keys are stored alongside for traceability.

### dim_customer (SCD Type 2)
| Column | Notes |
|---|---|
| customer_key | Surrogate PK — new value for each version |
| customer_id | Business key — constant across versions |
| effective_from | Date this version became active |
| effective_to | Date this version expired (NULL = current) |
| is_current | 1 = active version |
| row_hash | MD5 of tracked attributes |

**Tracked attributes** (change → new SCD2 version): `customer_segment`, `customer_city`, `customer_state`, `customer_country`, `customer_zipcode`, `customer_street`

**Non-tracked** (updated in-place): `customer_fname`, `customer_lname`, `customer_email`

### dim_supplier (SCD Type 2)
Same SCD2 pattern as `dim_customer`.

**Tracked attributes**: `performance_tier`, `composite_score`, `on_time_rate`

### Fact Table Joins
```sql
-- Correct join: resolve the dimension version active on the order date
select *
from warehouse.fct_orders f
join warehouse.dim_customer c
    on f.customer_key = c.customer_key   -- surrogate already pins the right version
join warehouse.dim_date d
    on f.order_date_key = d.date_key
```

---

## marts Schema

| Mart | Grain | Primary Audience |
|---|---|---|
| mart_supplier_performance | supplier × month | Procurement |
| mart_route_efficiency | shipping_mode × geography × month | Logistics |
| mart_customer_segment_value | customer × month (with RFM) | Marketing |
| mart_inventory_velocity | product × month | Supply Planning |
| mart_late_delivery_risk | order (live + predicted) | Operations / SLA |

---

## metadata Schema

### metadata.pipeline_runs
One row per Airflow task execution. Written by every pipeline task to enable run-level observability without querying Airflow's own metadata DB.

Key columns: `dag_id`, `task_id`, `execution_date`, `rows_read`, `rows_written`, `dq_pass_rate`, `status`.
