-- =============================================================================
-- 01_raw_tables.sql  —  Bronze / Raw Layer
-- All DataCo source columns stored as VARCHAR (no type coercion in raw layer).
-- Every row carries a load_batch_id + source_file + ingestion_ts for lineage.
-- Idempotent: IF NOT EXISTS guards allow re-running without errors.
-- =============================================================================

use `raw`;

-- ---------------------------------------------------------------------------
-- raw.orders
-- One row per order-item line as received from DataCo CSV.
-- Column names are snake_case mappings of the original CSV headers.
-- ---------------------------------------------------------------------------
create table if not exists `raw`.orders (
    id                          bigint          auto_increment primary key,

    -- Source columns (preserved as strings to prevent silent parse failures)
    type                        varchar(50),
    days_for_shipping_real      varchar(20),
    days_for_shipment_scheduled varchar(20),
    benefit_per_order           varchar(30),
    sales_per_customer          varchar(30),
    delivery_status             varchar(100),
    late_delivery_risk          varchar(10),
    category_id                 varchar(20),
    category_name               varchar(200),
    customer_city               varchar(100),
    customer_country            varchar(100),
    customer_email              varchar(200),
    customer_fname              varchar(100),
    customer_id                 varchar(20),
    customer_lname              varchar(100),
    customer_password           varchar(200),
    customer_segment            varchar(100),
    customer_state              varchar(100),
    customer_street             varchar(300),
    customer_zipcode            varchar(20),
    department_id               varchar(20),
    department_name             varchar(200),
    latitude                    varchar(30),
    longitude                   varchar(30),
    market                      varchar(100),
    order_city                  varchar(100),
    order_country               varchar(100),
    order_customer_id           varchar(20),
    order_date                  varchar(50),
    order_id                    varchar(20),
    order_item_cardprod_id      varchar(20),
    order_item_discount         varchar(30),
    order_item_discount_rate    varchar(30),
    order_item_id               varchar(20),
    order_item_product_price    varchar(30),
    order_item_profit_ratio     varchar(30),
    order_item_quantity         varchar(10),
    sales                       varchar(30),
    order_item_total            varchar(30),
    order_profit_per_order      varchar(30),
    order_region                varchar(100),
    order_state                 varchar(100),
    order_status                varchar(100),
    order_zipcode               varchar(20),
    product_card_id             varchar(20),
    product_category_id         varchar(20),
    product_description         text,
    product_image               varchar(500),
    product_name                varchar(300),
    product_price               varchar(30),
    product_status              varchar(20),
    shipping_date               varchar(50),
    shipping_mode               varchar(100),

    -- Batch metadata (set by the ingestion script)
    load_batch_id               varchar(50)     not null,
    source_file                 varchar(500)    not null,
    ingestion_ts                timestamp       default current_timestamp,

    index idx_order_id      (order_id),
    index idx_customer_id   (customer_id),
    index idx_load_batch_id (load_batch_id),
    index idx_ingestion_ts  (ingestion_ts)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;


-- ---------------------------------------------------------------------------
-- raw.load_log
-- One row per file load. Lets the pipeline skip re-loading the same file
-- and provides the source of truth for batch lineage.
-- ---------------------------------------------------------------------------
create table if not exists `raw`.load_log (
    id                bigint          auto_increment primary key,
    batch_id          varchar(50)     not null,
    source_file       varchar(500)    not null,
    rows_loaded       bigint,
    start_ts          timestamp,
    end_ts            timestamp,
    duration_seconds  int,
    status            varchar(20)     default 'SUCCESS',
    error_message     text,
    created_at        timestamp       default current_timestamp,

    index idx_batch_id    (batch_id),
    index idx_source_file (source_file),
    index idx_status      (status)
) engine = InnoDB
  default charset = utf8mb4
  collate = utf8mb4_unicode_ci;
