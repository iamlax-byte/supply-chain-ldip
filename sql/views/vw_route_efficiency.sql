-- =============================================================================
-- vw_route_efficiency
-- Audience: Tableau "Route Efficiency Heatmap" / Power BI Operations page
-- Grain: one row per shipping_mode × order_region × calendar month
--
-- Adds over mart_route_efficiency:
--   route_key         — concatenated label for viz tooltips
--   efficiency_label  — human-readable band
--   avg_delay_band    — bucketed delay for heat coloring (early / on-time / at-risk / critical)
--   late_rate_rank    — rank within month (1 = worst late rate)
-- =============================================================================

create or replace view marts.vw_route_efficiency as
with ranked as (
    select
        re.*,
        rank() over (
            partition by re.report_month
            order by re.late_delivery_rate desc
        ) as late_rate_rank,
        count(*) over (partition by re.report_month) as routes_in_month
    from marts.mart_route_efficiency re
)
select
    shipping_mode_key,
    geography_key,
    shipping_mode,
    order_region,
    report_month,
    total_orders,
    late_orders,
    on_time_orders,
    late_delivery_rate,
    avg_delivery_delay_days,
    avg_scheduled_days,
    avg_profit_per_order,
    efficiency_score,
    -- Composite route label for Tableau/PBI dimension axis
    concat(shipping_mode, ' › ', order_region)         as route_key,
    -- Efficiency tier for color coding
    case
        when efficiency_score >= 0.80 then 'High'
        when efficiency_score >= 0.60 then 'Medium'
        when efficiency_score >= 0.40 then 'Low'
        else 'Critical'
    end                                                as efficiency_label,
    -- Delay band drives heatmap color
    case
        when avg_delivery_delay_days < 0  then 'Early'
        when avg_delivery_delay_days <= 1 then 'On-Time'
        when avg_delivery_delay_days <= 3 then 'At-Risk'
        else 'Critical'
    end                                                as avg_delay_band,
    late_rate_rank,
    routes_in_month
from ranked;
