-- =============================================================================
-- 02_staging_tables.sql  —  Silver / Staging Layer
-- Typed, deduped, validated records ready for dimensional modelling.
-- Each staging table maps to a distinct entity extracted from the raw row.
-- =============================================================================

use staging;

-- ---------------------------------------------------------------------------
-- staging.stg_orders
-- One row per order (order_id grain). Typed and deduped from raw.orders.
-- Financial and delivery metrics are computed here; they flow to fct_orders.
-- ---------------------------------------------------------------------------
create table if not exists staging.stg_orders (
    order_id                    int             not null,
    order_customer_id           int,
    order_date                  datetime,
    order_status                varchar(100),
    order_type                  varchar(50),
    order_region                varchar(100),
    order_city                  varchar(100),
    order_state                 varchar(100),
    order_country               varchar(100),
    order_zipcode               varchar(20),
    shipping_date               datetime,
    shipping_mode               varchar(100),
    days_for_shipping_real      tinyint,
    days_for_shipment_scheduled tinyint,
    -- Derived delivery metrics
    delivery_delay_days         int,            -- actual_days - scheduled_days
    is_late_flag                tinyint(1),     -- 1 = late, 0 = on time
    late_delivery_risk          tinyint(1),     -- source flag from DataCo
    delivery_status             varchar(100),
    -- Financial metrics
    sales_amount                decimal(12,2),
    benefit_per_order           decimal(12,2),
    sales_per_customer          decimal(12,2),
    order_profit                decimal(12,2),
    profit_margin_pct           decimal(8,4),   -- order_profit / sales_amount
    -- Geography
    market                      varchar(100),
    latitude                    decimal(10,6),
    longitude                   decimal(10,6),
    -- Pipeline metadata
    load_batch_id               varchar(50),
    staged_at                   timestamp       default current_timestamp,

    primary key (order_id),
    index idx_order_date    (order_date),
    index idx_shipping_mode (shipping_mode),
    index idx_order_region  (order_region),
    index idx_is_late_flag  (is_late_flag),
    index idx_load_batch_id (load_batch_id)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------------
-- staging.stg_order_items
-- One row per order line item (order_item_id grain).
-- ---------------------------------------------------------------------------
create table if not exists staging.stg_order_items (
    order_item_id               int             not null,
    order_id                    int             not null,
    product_card_id             int,
    department_id               int,
    order_item_quantity         int,
    order_item_product_price    decimal(10,2),
    order_item_discount         decimal(10,2),
    order_item_discount_rate    decimal(8,4),
    order_item_total            decimal(12,2),
    order_item_profit_ratio     decimal(8,4),
    order_item_profit           decimal(12,2),  -- total * profit_ratio
    load_batch_id               varchar(50),
    staged_at                   timestamp       default current_timestamp,

    primary key (order_item_id),
    index idx_order_id      (order_id),
    index idx_product_id    (product_card_id),
    index idx_department_id (department_id)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------------
-- staging.stg_customers
-- One row per unique customer_id extracted from the raw order rows.
-- The SCD2 merge happens downstream in the warehouse layer.
-- ---------------------------------------------------------------------------
create table if not exists staging.stg_customers (
    customer_id       int             not null,
    customer_fname    varchar(100),
    customer_lname    varchar(100),
    customer_email    varchar(200),
    customer_segment  varchar(100),
    customer_city     varchar(100),
    customer_state    varchar(100),
    customer_country  varchar(100),
    customer_zipcode  varchar(20),
    customer_street   varchar(300),
    -- MD5 hash of tracked attributes; change triggers SCD2 new version
    row_hash          varchar(32),
    load_batch_id     varchar(50),
    staged_at         timestamp       default current_timestamp,

    primary key (customer_id),
    index idx_customer_segment (customer_segment),
    index idx_row_hash         (row_hash)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------------
-- staging.stg_products
-- One row per unique product_card_id. Updated in-place (SCD1) in warehouse.
-- ---------------------------------------------------------------------------
create table if not exists staging.stg_products (
    product_card_id    int            not null,
    product_name       varchar(300),
    product_price      decimal(10,2),
    product_status     varchar(20),
    category_id        int,
    category_name      varchar(200),
    department_id      int,
    department_name    varchar(200),
    product_image      varchar(500),
    load_batch_id      varchar(50),
    staged_at          timestamp      default current_timestamp,

    primary key (product_card_id),
    index idx_category_id   (category_id),
    index idx_department_id (department_id)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------------
-- staging.stg_suppliers
-- Supplier = Department in DataCo (Department Id / Department Name).
-- Supplier performance metrics (on_time_rate, composite_score) are computed
-- from order history during the staging transform and feed the SCD2 merge.
-- ---------------------------------------------------------------------------
create table if not exists staging.stg_suppliers (
    supplier_id        int            not null,   -- maps to department_id
    supplier_name      varchar(200),              -- maps to department_name
    supplier_market    varchar(100),
    on_time_rate       decimal(5,4),
    avg_profit_margin  decimal(8,4),
    composite_score    decimal(5,2),
    performance_tier   varchar(20),   -- 'platinum' | 'gold' | 'silver' | 'bronze'
    row_hash           varchar(32),
    load_batch_id      varchar(50),
    staged_at          timestamp      default current_timestamp,

    primary key (supplier_id),
    index idx_performance_tier (performance_tier)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------------
-- staging.dq_results
-- One row per DQ check execution. Written by the DQ framework (Phase 2).
-- Critical failures block downstream tasks via Airflow branching.
-- ---------------------------------------------------------------------------
create table if not exists staging.dq_results (
    id              bigint          auto_increment primary key,
    run_id          varchar(50)     not null,       -- links to metadata.pipeline_runs
    batch_id        varchar(50),
    rule_name       varchar(200)    not null,
    table_name      varchar(100),
    column_name     varchar(100),
    check_type      varchar(50),
    severity        varchar(20),                    -- 'critical' | 'warning'
    rows_checked    bigint,
    rows_failed     bigint,
    pass_rate       decimal(5,4),
    passed          tinyint(1),
    failure_detail  text,
    executed_at     timestamp       default current_timestamp,

    index idx_run_id    (run_id),
    index idx_batch_id  (batch_id),
    index idx_passed    (passed),
    index idx_severity  (severity)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;
