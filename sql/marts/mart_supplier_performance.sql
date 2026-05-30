-- =============================================================================
-- mart_supplier_performance.sql
-- Grain: one row per supplier × calendar month
-- Audience: Procurement / Vendor Management
-- Scheduled: daily (overwrites current month's rows)
--
-- Parameter: :report_month  — first day of the month (YYYY-MM-01)
-- =============================================================================

-- Idempotent: delete current month's data before re-inserting
delete from marts.mart_supplier_performance
where report_month = :report_month;

insert into marts.mart_supplier_performance (
    supplier_key, supplier_id, supplier_name, supplier_market,
    report_month,
    total_orders, on_time_orders, late_orders,
    on_time_rate, avg_delivery_delay,
    total_revenue, total_profit, avg_profit_margin,
    composite_score, performance_tier
)
select
    ds.supplier_key,
    ds.supplier_id,
    ds.supplier_name,
    ds.supplier_market,
    :report_month                                      as report_month,

    -- Volume
    count(distinct fo.order_id)                        as total_orders,
    sum(case when fo.is_late_flag = 0 then 1 else 0 end) as on_time_orders,
    sum(fo.is_late_flag)                               as late_orders,

    -- Rates
    round(
        sum(case when fo.is_late_flag = 0 then 1 else 0 end)
        / nullif(count(distinct fo.order_id), 0)
    , 4)                                               as on_time_rate,
    round(avg(fo.delivery_delay_days), 2)              as avg_delivery_delay,

    -- Financials
    round(sum(foi.order_item_total), 2)                as total_revenue,
    round(sum(foi.order_item_profit), 2)               as total_profit,
    round(avg(fo.profit_margin_pct), 4)                as avg_profit_margin,

    -- Performance score (mirrors derived_fields.py compute_supplier_score logic)
    round(
        ( sum(case when fo.is_late_flag = 0 then 1 else 0 end)
          / nullif(count(distinct fo.order_id), 0) ) * 0.50
        + avg(fo.profit_margin_pct) * 0.30
        + ( count(distinct fo.order_id)
            / nullif(max(count(distinct fo.order_id)) over (), 0) ) * 0.20
    , 2)                                               as composite_score,
    ds.performance_tier

from warehouse.fct_order_items foi
join warehouse.fct_orders fo
    on fo.order_id      = foi.order_id
join warehouse.dim_supplier ds
    on ds.supplier_key  = foi.supplier_key
   and ds.is_current    = 1
join warehouse.dim_date dd
    on dd.date_key      = fo.order_date_key
where date_format(dd.full_date, '%Y-%m-01') = :report_month

group by
    ds.supplier_key, ds.supplier_id, ds.supplier_name,
    ds.supplier_market, ds.performance_tier;
