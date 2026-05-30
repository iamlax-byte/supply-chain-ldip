-- =============================================================================
-- vw_customer_rfm_segments
-- Audience: Tableau / Power BI "Customer Segmentation" page
-- Grain: one row per customer × calendar month
--
-- Adds over mart_customer_segment_value:
--   r_score / f_score / m_score  — individual quintile scores extracted from rfm_score string
--   rfm_segment_label            — behavioural segment name (Champions, At Risk, etc.)
--   segment_priority             — sort order (1 = most valuable to retain)
--   clv_band                     — lifetime value tier for colour coding
-- =============================================================================

create or replace view marts.vw_customer_rfm_segments as
with parsed as (
    select
        csv.*,
        -- rfm_score is stored as a 3-char string e.g. '543'
        cast(substr(rfm_score, 1, 1) as unsigned) as r_score,
        cast(substr(rfm_score, 2, 1) as unsigned) as f_score,
        cast(substr(rfm_score, 3, 1) as unsigned) as m_score
    from marts.mart_customer_segment_value csv
),
segmented as (
    select
        p.*,
        -- Standard RFM segment mapping (adapt thresholds to business context)
        case
            when r_score >= 4 and f_score >= 4 and m_score >= 4 then 'Champions'
            when r_score >= 3 and f_score >= 3                   then 'Loyal Customers'
            when r_score >= 4 and f_score <= 2                   then 'Recent Customers'
            when r_score >= 3 and m_score >= 3                   then 'Potential Loyalists'
            when r_score <= 2 and f_score >= 3 and m_score >= 3  then 'At Risk'
            when r_score <= 2 and f_score >= 4                   then 'Cannot Lose Them'
            when r_score <= 2 and f_score <= 2                   then 'Hibernating'
            else 'Needs Attention'
        end as rfm_segment_label
    from parsed p
)
select
    customer_key,
    customer_id,
    customer_segment,
    report_month,
    total_orders,
    total_revenue,
    total_profit,
    avg_order_value,
    late_delivery_count,
    late_delivery_rate,
    recency_days,
    frequency_count,
    monetary_value,
    rfm_score,
    r_score,
    f_score,
    m_score,
    rfm_segment_label,
    -- Sort priority: highest-value segments first for executive views
    case rfm_segment_label
        when 'Champions'        then 1
        when 'Loyal Customers'  then 2
        when 'Potential Loyalists' then 3
        when 'Recent Customers' then 4
        when 'Cannot Lose Them' then 5
        when 'At Risk'          then 6
        when 'Needs Attention'  then 7
        else 8  -- Hibernating
    end as segment_priority,
    -- CLV band based on monetary_value
    case
        when monetary_value >= 10000 then 'Platinum'
        when monetary_value >=  5000 then 'Gold'
        when monetary_value >=  1000 then 'Silver'
        else 'Bronze'
    end as clv_band
from segmented;
