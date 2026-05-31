-- =============================================================================
-- mart_inventory_velocity.sql
-- Grain: one row per product × calendar month
-- Audience: Supply Planning / Category Management
--
-- Parameter: :report_month  — first day of the month (YYYY-MM-01)
-- =============================================================================

delete from marts.mart_inventory_velocity
where report_month = :report_month;

insert into marts.mart_inventory_velocity (
    product_key, product_card_id,
    category_id, category_name,
    department_id, department_name,
    report_month,
    units_sold, total_revenue, total_profit,
    avg_unit_price, avg_discount_rate,
    inventory_turnover_rate, velocity_tier
)
with category_totals as (
    -- Total units sold per category this month (for relative velocity calculation)
    select
        dp.category_id,
        sum(foi.order_item_quantity) as category_units
    from warehouse.fct_order_items foi
    join warehouse.dim_product dp
        on dp.product_key    = foi.product_key
    join warehouse.dim_date dd
        on dd.date_key       = foi.order_date_key
    where date_format(dd.full_date, '%Y-%m-01') = :report_month
    group by dp.category_id
),
product_sales as (
    select
        dp.product_key,
        dp.product_card_id,
        dp.category_id,
        dp.category_name,
        dp.department_id,
        dp.department_name,
        sum(foi.order_item_quantity)      as units_sold,
        round(sum(foi.order_item_total), 2)  as total_revenue,
        round(sum(foi.order_item_profit), 2) as total_profit,
        round(avg(foi.order_item_product_price), 2) as avg_unit_price,
        round(avg(foi.order_item_discount_rate), 4)  as avg_discount_rate
    from warehouse.fct_order_items foi
    join warehouse.dim_product dp
        on dp.product_key  = foi.product_key
    join warehouse.dim_date dd
        on dd.date_key     = foi.order_date_key
    where date_format(dd.full_date, '%Y-%m-01') = :report_month
    group by
        dp.product_key, dp.product_card_id, dp.category_id,
        dp.category_name, dp.department_id, dp.department_name
)
select
    ps.product_key,
    ps.product_card_id,
    ps.category_id,
    ps.category_name,
    ps.department_id,
    ps.department_name,
    :report_month                                          as report_month,
    ps.units_sold,
    ps.total_revenue,
    ps.total_profit,
    ps.avg_unit_price,
    ps.avg_discount_rate,
    -- Turnover rate: this product's units / average units across the category
    round(
        ps.units_sold
        / nullif(ct.category_units / nullif(
            (select count(distinct dp2.product_key)
             from warehouse.fct_order_items foi2
             join warehouse.dim_product dp2 on dp2.product_key = foi2.product_key
             join warehouse.dim_date dd2    on dd2.date_key    = foi2.order_date_key
             where date_format(dd2.full_date, '%Y-%m-01') = :report_month
               and dp2.category_id = ps.category_id
            ), 0), 0)
    , 4)                                                   as inventory_turnover_rate,
    -- Velocity tier based on units sold relative to category median
    case
        when ps.units_sold >= ct.category_units * 0.20 then 'fast'
        when ps.units_sold >= ct.category_units * 0.10 then 'medium'
        when ps.units_sold >  0                         then 'slow'
        else 'dead'
    end                                                    as velocity_tier

from product_sales ps
join category_totals ct on ct.category_id = ps.category_id;
