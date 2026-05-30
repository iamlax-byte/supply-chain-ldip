-- =============================================================================
-- 04_marts_tables.sql  —  Aggregated Business Marts
-- Materialized tables populated daily by Airflow mart tasks (Phase 4).
-- Each mart is a pre-aggregated view of warehouse data for a specific audience.
-- =============================================================================

use marts;

-- ---------------------------------------------------------------------------
-- marts.mart_supplier_performance
-- Audience: Procurement / Vendor Management
-- Grain: one row per supplier per calendar month
-- Populated by: sql/marts/mart_supplier_performance.sql
-- ---------------------------------------------------------------------------
create table if not exists marts.mart_supplier_performance (
    id                  bigint          auto_increment primary key,
    supplier_key        int             not null,
    supplier_id         int             not null,
    supplier_name       varchar(200),
    supplier_market     varchar(100),
    report_month        date            not null,   -- first day of month (YYYY-MM-01)
    -- Volume
    total_orders        int,
    on_time_orders      int,
    late_orders         int,
    -- Performance rates
    on_time_rate        decimal(5,4),
    avg_delivery_delay  decimal(8,2),
    -- Financials
    total_revenue       decimal(15,2),
    total_profit        decimal(15,2),
    avg_profit_margin   decimal(8,4),
    -- Composite score (weighted: on_time_rate × 0.5 + margin × 0.3 + volume × 0.2)
    composite_score     decimal(5,2),
    performance_tier    varchar(20),
    updated_at          timestamp       default current_timestamp on update current_timestamp,

    unique key uq_supplier_month (supplier_key, report_month),
    index idx_report_month      (report_month),
    index idx_performance_tier  (performance_tier),
    index idx_supplier_id       (supplier_id)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------------
-- marts.mart_route_efficiency
-- Audience: Logistics / Operations
-- Grain: one row per shipping_mode × geography × calendar month
-- Populated by: sql/marts/mart_route_efficiency.sql
-- ---------------------------------------------------------------------------
create table if not exists marts.mart_route_efficiency (
    id                  bigint          auto_increment primary key,
    shipping_mode_key   tinyint         not null,
    shipping_mode_name  varchar(50),
    geography_key       int             not null,
    order_region        varchar(100),
    order_country       varchar(100),
    market              varchar(100),
    report_month        date            not null,
    -- Volume
    total_shipments     int,
    on_time_shipments   int,
    late_shipments      int,
    -- Rates
    on_time_rate        decimal(5,4),
    avg_delay_days      decimal(8,2),
    avg_days_actual     decimal(8,2),
    avg_days_scheduled  decimal(8,2),
    -- Efficiency score: on_time_rate normalized against mode average
    efficiency_score    decimal(5,2),
    updated_at          timestamp       default current_timestamp on update current_timestamp,

    unique key uq_route_month (shipping_mode_key, geography_key, report_month),
    index idx_report_month  (report_month),
    index idx_order_region  (order_region),
    index idx_market        (market)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------------
-- marts.mart_customer_segment_value
-- Audience: Marketing / CX
-- Grain: one row per customer × calendar month (with RFM scoring)
-- Populated by: sql/marts/mart_customer_segment_value.sql
-- ---------------------------------------------------------------------------
create table if not exists marts.mart_customer_segment_value (
    id                      bigint          auto_increment primary key,
    customer_key            bigint          not null,
    customer_id             int             not null,
    customer_segment        varchar(100),
    report_month            date            not null,
    -- Transaction metrics
    total_orders            int,
    total_revenue           decimal(15,2),
    total_profit            decimal(15,2),
    avg_order_value         decimal(12,2),
    -- Service quality
    late_delivery_count     int,
    late_delivery_rate      decimal(5,4),
    -- RFM (Recency / Frequency / Monetary) — computed as of report_month end
    recency_days            int,            -- days since last order as of month end
    frequency_count         int,            -- lifetime order count
    monetary_value          decimal(15,2),  -- lifetime revenue
    rfm_score               varchar(10),    -- e.g. '555', '311'
    updated_at              timestamp       default current_timestamp on update current_timestamp,

    unique key uq_customer_month (customer_key, report_month),
    index idx_customer_segment (customer_segment),
    index idx_report_month     (report_month),
    index idx_rfm_score        (rfm_score)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------------
-- marts.mart_inventory_velocity
-- Audience: Supply Planning / Category Management
-- Grain: one row per product × calendar month
-- Populated by: sql/marts/mart_inventory_velocity.sql
-- ---------------------------------------------------------------------------
create table if not exists marts.mart_inventory_velocity (
    id                      bigint          auto_increment primary key,
    product_key             int             not null,
    product_card_id         int             not null,
    category_id             int,
    category_name           varchar(200),
    department_id           int,
    department_name         varchar(200),
    report_month            date            not null,
    -- Volume
    units_sold              int,
    total_revenue           decimal(15,2),
    total_profit            decimal(15,2),
    avg_unit_price          decimal(10,2),
    avg_discount_rate       decimal(5,4),
    -- Velocity (units_sold / avg units sold across category that month)
    inventory_turnover_rate decimal(8,4),
    velocity_tier           varchar(20),    -- 'fast' | 'medium' | 'slow' | 'dead'
    updated_at              timestamp       default current_timestamp on update current_timestamp,

    unique key uq_product_month (product_key, report_month),
    index idx_category_name (category_name),
    index idx_report_month  (report_month),
    index idx_velocity_tier (velocity_tier)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------------
-- marts.mart_late_delivery_risk
-- Audience: Operations / SLA Management
-- Grain: one row per order (live + predicted orders)
-- Populated by: ML classifier task in Phase 5
-- Actual outcomes back-filled once delivery completes.
-- ---------------------------------------------------------------------------
create table if not exists marts.mart_late_delivery_risk (
    id                          bigint          auto_increment primary key,
    order_id                    int             not null,
    order_key                   bigint,
    customer_key                bigint,
    geography_key               int,
    shipping_mode_key           tinyint,
    order_date_key              int,
    -- Input features used for prediction (stored for model explainability)
    days_for_shipment_scheduled tinyint,
    shipping_mode               varchar(100),
    order_region                varchar(100),
    customer_segment            varchar(100),
    product_category            varchar(200),
    order_item_quantity         int,
    -- Prediction output
    predicted_risk_score        decimal(5,4),   -- probability 0.0–1.0
    predicted_is_late           tinyint(1),     -- 1 = at-risk, 0 = on-track
    model_version               varchar(20),
    prediction_ts               timestamp,
    -- Actual outcome (NULL until delivery resolves)
    actual_is_late              tinyint(1),
    actual_delivery_delay_days  int,
    -- Metadata
    created_at                  timestamp       default current_timestamp,
    updated_at                  timestamp       default current_timestamp on update current_timestamp,

    unique key uq_order_id      (order_id),
    index idx_order_date_key    (order_date_key),
    index idx_predicted_is_late (predicted_is_late),
    index idx_risk_score        (predicted_risk_score),
    index idx_prediction_ts     (prediction_ts)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;
