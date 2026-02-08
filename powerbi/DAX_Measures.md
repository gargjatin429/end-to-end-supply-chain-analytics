# Power BI DAX Measures

All measures below are created in the '_key_measures' table
and are used across the dashboard pages.

These measures are created as explicit DAX measures (not calculated columns) to ensure:
- correct filter context behavior

- flexibility across visuals

- no duplication of logic in SQL
---

1️⃣ Delivery Performance Metrics

Avg Actual Days =
AVERAGE(Fact_Sales[days_for_shipping_real])

Avg Scheduled Days =
AVERAGE(Fact_Sales[days_for_shipment_scheduled])

Avg Shipping Days =
AVERAGE(Fact_Sales[days_for_shipping_real])

Purpose:
- Track real vs planned delivery performance.
- Used for SLA monitoring and executive summaries.
---

2️⃣ Order Volume & Revenue Baseline

Total Orders =
COUNTROWS(Fact_Sales)

Total Revenue =
SUM(Fact_Sales[net_revenue])

Purpose:
- Foundation measures. Everything else builds on these.
- Never hard-code counts or revenue into visuals.
---

3️⃣ Profitability & Loss Detection

Profit Margin % =
DIVIDE(
    SUM(Fact_Sales[order_profit_amount]),
    SUM(Fact_Sales[net_revenue]),
    0
)

Bleeder Loss =
CALCULATE(
    SUM(Fact_Sales[order_profit_amount]),
    Fact_Sales[is_profit_bleeder] = TRUE
)

Purpose:
- Expose margin health and isolate loss-making orders.
- This is where “high revenue, bad business” becomes visible.
---

4️⃣ Delivery Reliability Metrics

Late Delivery % =
VAR LateOrders =
    CALCULATE(
        COUNTROWS(Fact_Sales),
        Fact_Sales[delivery_class] = "Late"
    )
RETURN
DIVIDE(LateOrders, [Total Orders], 0)

On-Time Delivery % =
VAR OnTimeOrders =
    CALCULATE(
        COUNTROWS(Fact_Sales),
        Fact_Sales[delivery_class] IN {"Early", "On Time"}
    )
RETURN
DIVIDE(OnTimeOrders, [Total Orders], 0)

Purpose:
- Translate raw delivery flags into executive-friendly KPIs.
- Used heavily by Ops and Logistics leadership.
---

5️⃣ Fraud & Risk Exposure

Fraud Rate % =
VAR FraudOrders =
    CALCULATE(
        COUNTROWS(Fact_Sales),
        Fact_Sales[order_status] = "SUSPECTED_FRAUD"
    )
RETURN
DIVIDE(FraudOrders, [Total Orders], 0)

Return Rate % =
DIVIDE(
    CALCULATE(
        COUNTROWS(Fact_Sales),
        Fact_Sales[order_status] IN {"CANCELED", "SUSPECTED_FRAUD"}
    ),
    [Total Orders]
)

Purpose:
- Quantify operational leakage caused by fraud and failed orders.
- Supports risk and compliance narratives.
---

6️⃣ Portfolio & Market Structure Metrics

Category Share % =
VAR CurrentRevenue = [Total Revenue]
VAR CategoryTotal =
    CALCULATE(
        [Total Revenue],
        REMOVEFILTERS(Dim_Product[product_name]),
        VALUES(Dim_Product[category_name])
    )
RETURN
DIVIDE(CurrentRevenue, CategoryTotal)

Market Share % =
VAR CurrentRevenue = [Total Revenue]
VAR MarketTotal =
    CALCULATE(
        [Total Revenue],
        REMOVEFILTERS(Fact_Sales),
        REMOVEFILTERS(Dim_Product),
        VALUES(Dim_Geo[market])
    )
RETURN
DIVIDE(CurrentRevenue, MarketTotal)

Purpose:
- Show concentration risk and revenue dependency.
- Designed to answer “where are we actually strong vs just busy?”
---

7️⃣ Order Lifecycle Grouping (Calculated Column)

order_status_group =
SWITCH(
    TRUE(),
    Fact_Sales[order_status] IN {"COMPLETE", "CLOSED"}, "Success",
    Fact_Sales[order_status] IN {
        "PENDING", "PROCESSING", "PENDING_PAYMENT",
        "PAYMENT_REVIEW", "ON_HOLD"
    }, "In Flight",
    Fact_Sales[order_status] IN {"CANCELED", "SUSPECTED_FRAUD"}, "Failure",
    "Other"
)

Purpose:
- Human-readable lifecycle grouping for dashboards.
- This should be a calculated column, not a measure. you placed it correctly.
---