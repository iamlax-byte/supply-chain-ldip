# Tableau Public Dashboard Specification

## Dashboard 1 — Supplier Scorecard

**Data source:** `marts.vw_supplier_scorecard`

### Sheets

| Sheet | Chart type | Dimensions | Measures | Notes |
|---|---|---|---|---|
| Tier Distribution | Treemap | supplier_name, performance_tier | composite_score | Color = tier_color |
| Score Trend | Line chart | report_month | composite_score | One line per supplier; filter to top 10 |
| Supplier Table | Text table | supplier_name, report_month | on_time_rate, avg_profit_margin, composite_score, score_trend | Conditional format on score_trend |
| Tier Funnel | Bar | performance_tier | COUNT(supplier_id) | Sort Platinum→Gold→Silver→Bronze |
| Top Movers | Bullet chart | supplier_name | composite_score, prev_month_score | Sort by abs(score_delta) desc |

### Dashboard layout
```
┌────────────────────────────────────────────────────────┐
│  SUPPLIER SCORECARD          [Month filter] [Tier filter]│
├──────────────────┬─────────────────────────────────────┤
│  Tier Funnel     │  Score Trend (top 5 suppliers)       │
│  (narrow left)   │  (wide right)                        │
├──────────────────┴────────────┬────────────────────────┤
│  Treemap — all suppliers      │  Top Movers             │
│  colored by tier              │  (bullet chart)         │
├───────────────────────────────┴────────────────────────┤
│  Full supplier table (scrollable)                        │
└────────────────────────────────────────────────────────┘
```

### Parameters / filters
- `report_month` — date range slider
- `performance_tier` — multi-select (Platinum / Gold / Silver / Bronze)
- `supplier_name` — search box

---

## Dashboard 2 — Late Delivery Risk Heatmap

**Data source:** `marts.vw_late_delivery_risk_live` joined to `marts.vw_route_efficiency`

### Sheets

| Sheet | Chart type | Dimensions | Measures | Notes |
|---|---|---|---|---|
| Risk Heatmap | Heat map | shipping_mode (cols), order_region (rows) | AVG(predicted_risk_score) | Color = risk_color gradient |
| Risk Distribution | Bar | risk_band | COUNT(order_id) | Red/Amber/Green |
| At-Risk Orders | Text table | order_id, shipping_mode, order_region, customer_segment | predicted_risk_score, days_overdue | Filter: risk_band = 'High' |
| Risk Over Time | Area chart | report_month | COUNT(order_id) by risk_band | Stacked |
| Risk Driver | Horizontal bar | risk_driver | COUNT(order_id) | Sorted desc |

### Dashboard layout
```
┌────────────────────────────────────────────────────────┐
│  LATE DELIVERY RISK          [Month] [Region] [Mode]    │
├────────────────────────┬───────────────────────────────┤
│  Risk Heatmap          │  Risk Distribution             │
│  (mode × region grid)  │  (H/M/L bar)                  │
├────────────────────────┴───────────────────────────────┤
│  Risk Over Time (area chart — full width)               │
├──────────────────────────────────┬─────────────────────┤
│  At-Risk Orders table            │  Risk Driver bars    │
│  (filterable, sortable)          │                      │
└──────────────────────────────────┴─────────────────────┘
```

### Parameters / filters
- `report_month` — single-month picker
- `risk_band` — High / Medium / Low
- `shipping_mode` — multi-select
- `order_region` — multi-select

---

## Publishing to Tableau Public

1. Connect Tableau Desktop to MySQL: `localhost:3306`, user `ldip_user`
2. Set live connection to `marts` schema
3. Build sheets using the spec above
4. Server → Tableau Public → Save to Tableau Public
5. Copy the public URL and add to README `## Dashboards` section
6. Take a screenshot (`Cmd+Shift+3`) and save to `dashboards/tableau/supplier_scorecard.png`
