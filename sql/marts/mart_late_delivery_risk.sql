-- =============================================================================
-- mart_late_delivery_risk.sql
-- Grain: one row per order (live orders + predicted risk)
-- Audience: Operations / SLA Management
--
-- Phase 3: Populates feature columns only — predicted_risk_score and
--          predicted_is_late are NULL until the Phase 5 ML classifier runs.
-- Phase 5: The ML task will UPDATE these rows with predictions after this
--          INSERT, so the table always holds the latest feature + prediction state.
--
-- Parameter: :report_month  — first day of the month (YYYY-MM-01)
-- =============================================================================

-- Replace this month's risk rows before re-inserting (predictions get refreshed daily)
delete from marts.mart_late_delivery_risk
where order_date_key in (
    select date_key from warehouse.dim_date
    where date_format(full_date, '%Y-%m-01') = :report_month
);

insert into marts.mart_late_delivery_risk (
    order_id, order_key, customer_key, geography_key,
    shipping_mode_key, order_date_key,
    days_for_shipment_scheduled, shipping_mode,
    order_region, customer_segment, product_category,
    order_item_quantity,
    -- Prediction columns intentionally NULL here — filled by ML task (Phase 5)
    predicted_risk_score, predicted_is_late,
    model_version, prediction_ts,
    -- Actual outcome (back-filled once delivery resolves)
    actual_is_late, actual_delivery_delay_days
)
select
    fo.order_id,
    fo.order_key,
    fo.customer_key,
    fo.geography_key,
    fo.shipping_mode_key,
    fo.order_date_key,
    fo.days_for_shipment_scheduled,
    fo.delivery_status,                     -- shipping_mode
    fo.order_region,
    dc.customer_segment,
    any_value(dp.category_name)             as product_category,
    sum(foi.order_item_quantity)            as order_item_quantity,
    -- ML predictions (Phase 5)
    null                                    as predicted_risk_score,
    null                                    as predicted_is_late,
    null                                    as model_version,
    null                                    as prediction_ts,
    -- Actual outcome
    fo.is_late_flag                         as actual_is_late,
    fo.delivery_delay_days                  as actual_delivery_delay_days

from warehouse.fct_orders fo
join warehouse.dim_customer dc
    on dc.customer_key    = fo.customer_key
join warehouse.dim_date dd
    on dd.date_key        = fo.order_date_key
left join warehouse.fct_order_items foi
    on foi.order_id       = fo.order_id
left join warehouse.dim_product dp
    on dp.product_key     = foi.product_key

where date_format(dd.full_date, '%Y-%m-01') = :report_month

group by
    fo.order_id, fo.order_key, fo.customer_key, fo.geography_key,
    fo.shipping_mode_key, fo.order_date_key,
    fo.days_for_shipment_scheduled, fo.delivery_status,
    fo.order_region, dc.customer_segment,
    fo.is_late_flag, fo.delivery_delay_days;
