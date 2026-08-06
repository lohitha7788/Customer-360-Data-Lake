Query 1 — Top 10 customers by order value (Save as: top-customers-by-value)
SELECT customer_id,
       COUNT(order_id) AS total_orders,
       ROUND(SUM(total_amount), 2) AS total_spent
FROM customer360_db.bronze_orders
WHERE status = 'completed'
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

Query 2 — Clickstream events  (Save as: clickstream-events)
SELECT event_type, COUNT(*) AS event_count
FROM customer360_db.bronze_clickstream
GROUP BY event_type
ORDER BY event_count DESC;

Query 3 — Customers by segment from CRM (Save as: customers-by-segment)
SELECT segment, COUNT(*) AS customer_count,
       ROUND(AVG(lifetime_value), 2) AS avg_lifetime_value
FROM customer360_db.bronze_salesforce_crm
GROUP BY segment
ORDER BY customer_count DESC;

Query 4 — Silver orders data quality check (Save as: silver-orders-quality)
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS unique_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) AS null_amounts,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS null_dates
FROM customer360_db.silver_orders;

Query 5 — Monthly order revenue trend (Save as: monthly-revenue-trend)
SELECT order_year, order_month,
       COUNT(order_id) AS order_count,
       ROUND(SUM(total_amount), 2) AS total_revenue
FROM customer360_db.silver_orders
GROUP BY order_year, order_month
ORDER BY order_year DESC, order_month DESC;
