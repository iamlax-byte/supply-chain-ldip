-- =============================================================================
-- vw_supplier_scorecard
-- Audience: Tableau "Supplier Performance" dashboard / Power BI Supplier page
-- Grain: one row per supplier × calendar month
--
-- Adds over mart_supplier_performance:
--   tier_rank        — rank within the month (1 = best composite score)
--   tier_color       — hex for Tableau/PBI conditional formatting
--   prev_month_score — composite score from the prior calendar month
--   score_trend      — 'improving', 'declining', 'stable', 'new'
-- =============================================================================

create or replace view marts.vw_supplier_scorecard as
with base as (
    select
        sp.*,
        lag(sp.composite_score) over (
            partition by sp.supplier_id
            order by sp.report_month
        ) as prev_month_score
    from marts.mart_supplier_performance sp
),
ranked as (
    select
        b.*,
        rank() over (
            partition by b.report_month
            order by b.composite_score desc
        ) as tier_rank,
        case b.performance_tier
            when 'platinum' then '#1A73E8'   -- blue
            when 'gold'     then '#FBBC04'   -- yellow
            when 'silver'   then '#9AA0A6'   -- grey
            else                 '#D93025'   -- red (bronze)
        end as tier_color,
        case
            when b.prev_month_score is null                          then 'new'
            when b.composite_score > b.prev_month_score + 0.02      then 'improving'
            when b.composite_score < b.prev_month_score - 0.02      then 'declining'
            else 'stable'
        end as score_trend
    from base b
)
select
    supplier_key,
    supplier_id,
    supplier_name,
    report_month,
    total_orders,
    on_time_deliveries,
    late_deliveries,
    on_time_rate,
    avg_profit_margin,
    total_revenue,
    volume_score,
    composite_score,
    performance_tier,
    tier_rank,
    tier_color,
    round(prev_month_score, 4)  as prev_month_score,
    round(composite_score - coalesce(prev_month_score, composite_score), 4) as score_delta,
    score_trend
from ranked;
