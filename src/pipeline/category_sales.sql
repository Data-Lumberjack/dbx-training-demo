-- category_sales: sprzedaż na dzień i kategorię produktu.
CREATE OR REFRESH MATERIALIZED VIEW category_sales
COMMENT "Sprzedaż na dzień i kategorię"
AS SELECT
  order_date,
  category,
  SUM(quantity) AS units_sold,
  SUM(amount) AS total_amount
FROM orders_clean
GROUP BY order_date, category;
