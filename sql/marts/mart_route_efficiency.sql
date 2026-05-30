-- =============================================================================
-- mart_route_efficiency.sql
-- Grain: one row per shipping_mode × geography × calendar month
-- Audience: Logistics / Operations
--
-- Parameter: :report_month  — first day of the month (YYYY-MM-01)
-- =============================================================================

delete from marts.mart_route_efficiency
where report_month = :report_month;

insert into marts.mart_route_efficiency (
    shipping_mode_key, shipping_mode_name,
    geography_key, order_region, order_country, market,
    report_month,
    total_shipments, on_time_shipments, late_shipments,
    on_time_rate, avg_delay_days, avg_days_actual, avg_days_scheduled,
    efficiency_score
)
with mode_averages as (
    -- Pre-compute per-mode on-time rate for efficiency score normalisation
    select
        fs.shipping_mode_key,
        round(
            sum(case when fs.is_late_flag = 0 then 1 else 0 end)
            / nullif(count(*), 0)
        , 4) as mode_avg_on_time
    from warehouse.fct_shipments fs
    join warehouse.dim_date dd on dd.date_key = fs.order_date_key
    where date_format(dd.full_date, '%Y-%m-01') = :report_month
    group by fs.shipping_mode_key
)
select
    fs.shipping_mode_key,
    dsm.shipping_mode_name,
    fs.geography_key,
    dg.order_region,
    dg.order_country,
    dg.market,
    :report_month                                       as report_month,

    count(*)                                            as total_shipments,
    sum(case when fs.is_late_flag = 0 then 1 else 0 end) as on_time_shipments,
    sum(fs.is_late_flag)                                as late_shipments,

    round(
        sum(case when fs.is_late_flag = 0 then 1 else 0 end) / nullif(count(*), 0)
    , 4)                                                as on_time_rate,
    round(avg(fs.delivery_delay_days), 2)               as avg_delay_days,
    round(avg(fs.days_for_shipping_real), 2)            as avg_days_actual,
    round(avg(fs.days_for_shipment_scheduled), 2)       as avg_days_scheduled,

    -- Efficiency score: route on-time rate vs. the mode's overall average
    -- Score > 1.0 → this route outperforms the mode average
    round(
        ( sum(case when fs.is_late_flag = 0 then 1 else 0 end) / nullif(count(*), 0) )
        / nullif(ma.mode_avg_on_time, 0)
    , 2)                                                as efficiency_score

from warehouse.fct_shipments fs
join warehouse.dim_date dd
    on dd.date_key             = fs.order_date_key
join warehouse.dim_geography dg
    on dg.geography_key        = fs.geography_key
join warehouse.dim_shipping_mode dsm
    on dsm.shipping_mode_key   = fs.shipping_mode_key
join mode_averages ma
    on ma.shipping_mode_key    = fs.shipping_mode_key
where date_format(dd.full_date, '%Y-%m-01') = :report_month

group by
    fs.shipping_mode_key, dsm.shipping_mode_name,
    fs.geography_key, dg.order_region, dg.order_country, dg.market,
    ma.mode_avg_on_time;
