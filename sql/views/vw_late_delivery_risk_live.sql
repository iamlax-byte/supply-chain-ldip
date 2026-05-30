-- =============================================================================
-- vw_late_delivery_risk_live
-- Audience: Tableau / Power BI "Operations Risk" dashboard
-- Grain: one row per order (most recent report_month per order)
--
-- Provides:
--   risk_band          — 'High' / 'Medium' / 'Low' derived from ML score
--                        (falls back to feature-based heuristic when ML not yet run)
--   risk_color         — hex for conditional formatting
--   days_overdue       — positive = already breached SLA; negative = days remaining
--   risk_driver        — primary feature driving risk (for tooltip / narrative)
-- =============================================================================

create or replace view marts.vw_late_delivery_risk_live as
with latest_per_order as (
    -- If a backfill re-inserts the same order into multiple report_months,
    -- surface only the most recent row per order.
    select
        ldr.*,
        row_number() over (
            partition by ldr.order_id
            order by dd.full_date desc
        ) as rn
    from marts.mart_late_delivery_risk ldr
    join warehouse.dim_date dd
        on dd.date_key = ldr.order_date_key
),
risk_classified as (
    select
        lpo.*,
        -- Risk band: prefer ML score if available, else heuristic
        case
            when predicted_risk_score is not null then
                case
                    when predicted_risk_score >= 0.70 then 'High'
                    when predicted_risk_score >= 0.40 then 'Medium'
                    else 'Low'
                end
            -- Heuristic fallback: Standard shipping + high quantity → higher risk
            when shipping_mode in ('Standard Class', 'Second Class')
                 and order_item_quantity >= 3 then 'High'
            when shipping_mode = 'Second Class' then 'Medium'
            else 'Low'
        end as risk_band,
        -- Primary risk driver for operations tooltip
        case
            when predicted_risk_score >= 0.70              then 'ML: high predicted probability'
            when shipping_mode = 'Standard Class'
                 and days_for_shipment_scheduled >= 5      then 'Long scheduled window'
            when order_item_quantity >= 5                  then 'High quantity order'
            when customer_segment = 'Consumer'             then 'Consumer segment history'
            else 'No dominant driver'
        end as risk_driver
    from latest_per_order lpo
    where rn = 1
)
select
    order_id,
    order_key,
    customer_key,
    geography_key,
    shipping_mode_key,
    order_date_key,
    days_for_shipment_scheduled,
    shipping_mode,
    order_region,
    customer_segment,
    product_category,
    order_item_quantity,
    predicted_risk_score,
    predicted_is_late,
    model_version,
    prediction_ts,
    actual_is_late,
    actual_delivery_delay_days,
    risk_band,
    risk_driver,
    -- Hex colour for operational dashboards
    case risk_band
        when 'High'   then '#C5221F'   -- red
        when 'Medium' then '#E37400'   -- amber
        else               '#137333'   -- green
    end as risk_color,
    -- Positive = days already overdue; negative = days remaining before scheduled
    coalesce(actual_delivery_delay_days, 0) as days_overdue
from risk_classified;
