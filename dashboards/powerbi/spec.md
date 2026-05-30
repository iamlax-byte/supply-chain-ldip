# Power BI Desktop Dashboard Specification

## Report: Supply Chain Executive Intelligence

**Data sources:**
| Table | View |
|---|---|
| Supplier KPIs | `marts.vw_supplier_scorecard` |
| Route Efficiency | `marts.vw_route_efficiency` |
| Customer Segments | `marts.vw_customer_rfm_segments` |
| Product Velocity | `marts.vw_inventory_velocity` |
| Risk Live | `marts.vw_late_delivery_risk_live` |

**MySQL connection string:**
```
Server: localhost
Port: 3306
Database: marts
User: ldip_user
```

---

## Page 1 — Executive Summary

**Purpose:** C-suite one-glance view of supply chain health.

| Visual | Type | Fields | Notes |
|---|---|---|---|
| KPI card — On-Time Rate | Card | AVG(on_time_rate) from vw_supplier_scorecard | Green/Red conditional icon |
| KPI card — High-Risk Orders | Card | COUNTROWS where risk_band='High' | From vw_late_delivery_risk_live |
| KPI card — Avg Profit Margin | Card | AVG(avg_profit_margin) | From vw_supplier_scorecard |
| KPI card — Active Suppliers | Card | DISTINCTCOUNT(supplier_id) | |
| Supplier Tier Donut | Donut | performance_tier (legend), COUNT | Colors: #1A73E8/#FBBC04/#9AA0A6/#D93025 |
| Late Rate Trend | Line | report_month (axis), late_delivery_rate (values) | From vw_route_efficiency |
| Top 5 Risky Routes | Table | route_key, late_delivery_rate, avg_delivery_delay_days | Sorted desc by late_delivery_rate |
| Revenue by Segment | Bar | customer_segment, total_revenue | From vw_customer_rfm_segments |

**Slicers:** `report_month` (date range)

---

## Page 2 — Supplier Deep-Dive

**Purpose:** Procurement team — identify degrading suppliers before SLA breach.

| Visual | Type | Fields |
|---|---|---|
| Supplier Performance Table | Matrix | supplier_name (rows), report_month (cols), composite_score (values) |
| Score Trend Line | Line | report_month (axis), composite_score (values), supplier_name (legend) |
| Tier Change Waterfall | Waterfall | report_month (category), score_delta (values) |
| Drill-through table | Table | All columns from vw_supplier_scorecard for selected supplier |

**Drill-through:** Right-click any supplier → "See Orders" → jumps to Page 5 (Order Detail) filtered to that supplier.

---

## Page 3 — Route Efficiency

**Purpose:** Operations — which routes are chronically late?

| Visual | Type | Fields |
|---|---|---|
| Heatmap Matrix | Matrix | shipping_mode (rows), order_region (cols), avg_delivery_delay_days (values) | Conditional format: red = high delay |
| Late Rate Scatter | Scatter | avg_scheduled_days (X), late_delivery_rate (Y), total_orders (size), shipping_mode (color) |
| Efficiency Map | Filled map | order_region, efficiency_score | Requires region→lat-long table |
| Mode Comparison | Clustered bar | shipping_mode, avg_delivery_delay_days, on_time_rate |

---

## Page 4 — Customer Segments

**Purpose:** Marketing — identify high-value segments and at-risk customers.

| Visual | Type | Fields |
|---|---|---|
| RFM Segment Cards | Card row | Champions count, At Risk count, Cannot Lose Them count |
| Segment Revenue Treemap | Treemap | rfm_segment_label (group), total_revenue (size) |
| RFM Scatter | Scatter | recency_days (X), frequency_count (Y), monetary_value (size), rfm_segment_label (color) |
| Late Delivery by Segment | Bar | customer_segment, late_delivery_rate | Sorted desc |
| Customer Table | Table | customer_id, rfm_segment_label, rfm_score, monetary_value, late_delivery_rate | Filterable |

---

## Page 5 — Order Detail (Drill-Through)

**Purpose:** Ops team — inspect individual at-risk orders.

| Visual | Type | Fields |
|---|---|---|
| Order Risk Table | Table | order_id, shipping_mode, order_region, predicted_risk_score, risk_band, days_overdue, risk_driver |
| Risk Band Gauge | Gauge | COUNT high-risk / total orders |
| Risk Score Distribution | Histogram | predicted_risk_score (bins 0.0→1.0) |

**Filters:** risk_band, shipping_mode, order_region

---

## Publishing & Export

1. Open Power BI Desktop → Get Data → MySQL Database
2. Connect using the connection string above
3. Load all 5 views
4. Build pages using spec above
5. File → Export → Export to PDF for screenshots
6. Publish to Power BI Service (if licensed) or export PDF
7. Save screenshots to `dashboards/powerbi/*.png` and reference in README

### DAX Measures to create

```dax
[On-Time Rate] = DIVIDE(
    SUMX(vw_supplier_scorecard, vw_supplier_scorecard[on_time_deliveries]),
    SUMX(vw_supplier_scorecard, vw_supplier_scorecard[total_orders]),
    0
)

[High Risk Order Count] = CALCULATE(
    COUNTROWS(vw_late_delivery_risk_live),
    vw_late_delivery_risk_live[risk_band] = "High"
)

[Avg Composite Score] = AVERAGE(vw_supplier_scorecard[composite_score])

[Late Delivery Rate] = DIVIDE(
    SUMX(vw_route_efficiency, vw_route_efficiency[late_orders]),
    SUMX(vw_route_efficiency, vw_route_efficiency[total_orders]),
    0
)
```
