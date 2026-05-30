-- =============================================================================
-- vw_inventory_velocity
-- Audience: Tableau / Power BI "Product Velocity" page
-- Grain: one row per product × calendar month
--
-- Adds over mart_inventory_velocity:
--   profit_margin_pct       — total_profit / total_revenue (%)
--   revenue_rank_in_dept    — rank by revenue within department × month
--   units_rank_in_category  — rank by units_sold within category × month
--   velocity_color          — hex for Tableau/PBI conditional formatting
-- =============================================================================

create or replace view marts.vw_inventory_velocity as
with ranked as (
    select
        iv.*,
        rank() over (
            partition by iv.department_id, iv.report_month
            order by iv.total_revenue desc
        ) as revenue_rank_in_dept,
        rank() over (
            partition by iv.category_id, iv.report_month
            order by iv.units_sold desc
        ) as units_rank_in_category,
        sum(iv.units_sold) over (
            partition by iv.department_id, iv.report_month
        ) as dept_total_units
    from marts.mart_inventory_velocity iv
)
select
    product_key,
    product_card_id,
    category_id,
    category_name,
    department_id,
    department_name,
    report_month,
    units_sold,
    total_revenue,
    total_profit,
    avg_unit_price,
    avg_discount_rate,
    inventory_turnover_rate,
    velocity_tier,
    -- Profit margin (NULL-safe: zero revenue → NULL)
    round(total_profit / nullif(total_revenue, 0), 4)  as profit_margin_pct,
    -- Percentage of department total units
    round(units_sold / nullif(dept_total_units, 0), 4) as dept_unit_share_pct,
    revenue_rank_in_dept,
    units_rank_in_category,
    -- Hex for conditional formatting in Tableau / Power BI
    case velocity_tier
        when 'fast'   then '#137333'   -- dark green
        when 'medium' then '#E37400'   -- amber
        when 'slow'   then '#C5221F'   -- red
        else               '#5F6368'   -- grey (dead stock)
    end as velocity_color
from ranked;
