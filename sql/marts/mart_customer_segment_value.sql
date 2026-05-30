-- =============================================================================
-- mart_customer_segment_value.sql
-- Grain: one row per customer × calendar month
-- Audience: Marketing / CX
-- Includes RFM scoring (Recency / Frequency / Monetary) as of month-end.
--
-- Parameter: :report_month  — first day of the month (YYYY-MM-01)
-- =============================================================================

delete from marts.mart_customer_segment_value
where report_month = :report_month;

insert into marts.mart_customer_segment_value (
    customer_key, customer_id, customer_segment,
    report_month,
    total_orders, total_revenue, total_profit, avg_order_value,
    late_delivery_count, late_delivery_rate,
    recency_days, frequency_count, monetary_value,
    rfm_score
)
with month_end as (
    -- Last day of the report month (used for recency calculation)
    select last_day(:report_month) as month_end_date
),
monthly_activity as (
    select
        dc.customer_key,
        dc.customer_id,
        dc.customer_segment,
        count(distinct fo.order_id)              as total_orders,
        round(sum(fo.sales_amount), 2)           as total_revenue,
        round(sum(fo.order_profit), 2)           as total_profit,
        round(avg(fo.sales_amount), 2)           as avg_order_value,
        sum(fo.is_late_flag)                     as late_delivery_count,
        max(dd.full_date)                        as last_order_date
    from warehouse.fct_orders fo
    join warehouse.dim_customer dc
        on dc.customer_key = fo.customer_key
    join warehouse.dim_date dd
        on dd.date_key     = fo.order_date_key
    where date_format(dd.full_date, '%Y-%m-01') = :report_month
    group by dc.customer_key, dc.customer_id, dc.customer_segment
),
lifetime_stats as (
    -- Lifetime totals for RFM frequency + monetary (all-time, not just this month)
    select
        fo.customer_key,
        count(distinct fo.order_id)   as lifetime_orders,
        sum(fo.sales_amount)          as lifetime_revenue
    from warehouse.fct_orders fo
    group by fo.customer_key
),
rfm_raw as (
    select
        ma.*,
        ls.lifetime_orders,
        ls.lifetime_revenue,
        datediff(me.month_end_date, ma.last_order_date) as recency_days
    from monthly_activity ma
    join lifetime_stats ls on ls.customer_key = ma.customer_key
    cross join month_end me
),
rfm_scored as (
    -- Quintile scores 1–5 (5 = best) using NTILE window function
    select
        *,
        -- R: lower recency_days is better (score 5 = most recent)
        6 - ntile(5) over (order by recency_days  asc)  as r_score,
        -- F: higher frequency is better
        ntile(5)     over (order by lifetime_orders asc) as f_score,
        -- M: higher monetary is better
        ntile(5)     over (order by lifetime_revenue asc) as m_score
    from rfm_raw
)
select
    customer_key,
    customer_id,
    customer_segment,
    :report_month                                             as report_month,
    total_orders,
    total_revenue,
    total_profit,
    avg_order_value,
    late_delivery_count,
    round(late_delivery_count / nullif(total_orders, 0), 4)  as late_delivery_rate,
    recency_days,
    lifetime_orders                                           as frequency_count,
    round(lifetime_revenue, 2)                                as monetary_value,
    concat(r_score, f_score, m_score)                         as rfm_score
from rfm_scored;
