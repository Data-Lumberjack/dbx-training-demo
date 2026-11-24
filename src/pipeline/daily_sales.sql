-- daily_sales: sprzedaż na dzień, aktualizowana przy każdym odświeżeniu pipeline'u.
CREATE OR REFRESH MATERIALIZED VIEW daily_sales
COMMENT "Sprzedaż na dzień"
AS SELECT
  order_date,
  COUNT(*) AS orders_count,
  COUNT(DISTINCT customer_id) AS customers_count,
  SUM(amount) AS total_amount,
  ROUND(SUM(amount) / COUNT(*), 2) AS avg_order_amount
FROM orders_clean
GROUP BY order_date;
