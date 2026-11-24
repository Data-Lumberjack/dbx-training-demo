-- orders_clean: nowe wiersze z raw_orders, sprawdzone regułami jakości.
-- Tabela strumieniowa czyta przy każdym odświeżeniu tylko wiersze dopisane od poprzedniego, dlatego do raw_orders
-- zadanie ingest_orders tylko dopisuje. Nazwa bez katalogu i schematu wskazuje domyślny katalog i schemat pipeline'u.
CREATE OR REFRESH STREAMING TABLE orders_clean (
  -- ON VIOLATION DROP ROW: wiersz, który łamie regułę, nie trafia do tabeli
  CONSTRAINT has_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  -- bez ON VIOLATION: wiersz zostaje, a pipeline liczy go w metrykach jakości
  CONSTRAINT known_region EXPECT (region IN ('Warszawa', 'Kraków', 'Wrocław', 'Gdańsk', 'Poznań'))
)
COMMENT "Zamówienia po kontroli jakości"
AS SELECT
  order_id,
  order_ts,
  CAST(order_ts AS DATE) AS order_date,
  customer_id,
  product,
  category,
  quantity,
  unit_price,
  quantity * unit_price AS amount,
  region,
  batch_id,
  ingested_at
FROM STREAM(raw_orders);
