# Databricks notebook source
# MAGIC %md
# MAGIC ## ingest_orders: partia zamówień z jednego dnia
# MAGIC Zadanie dopisuje do tabeli `raw_orders` partię zamówień z dnia `run_date`. W firmie zamówienia przychodziłyby
# MAGIC z systemu sprzedaży, a tu generuje je kod. Cztery wiersze są celowo błędne: trzy pipeline odrzuci,
# MAGIC a czwarty przepuści, ale reguła jakości go policzy.

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

# Parametry joba. Przy ręcznym uruchomieniu notebooka działają wartości domyślne widżetów.
dbutils.widgets.text("catalog", "szkolenie")
dbutils.widgets.text("schema", "demo_dev")
dbutils.widgets.text("run_date", "")  # pusty: dzisiejsza data (UTC)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date_text = dbutils.widgets.get("run_date") or datetime.now(timezone.utc).date().isoformat()
run_date = datetime.strptime(run_date_text, "%Y-%m-%d").date()
table = f"{catalog}.{schema}.raw_orders"

# COMMAND ----------

# Katalog produktów: (produkt, kategoria, cena jednostkowa w zł)
PRODUCTS = [
    ("laptop", "elektronika", Decimal("3899.00")),
    ("monitor", "elektronika", Decimal("1149.00")),
    ("mysz", "akcesoria", Decimal("89.00")),
    ("klawiatura", "akcesoria", Decimal("249.00")),
    ("słuchawki", "akcesoria", Decimal("399.00")),
    ("biurko", "meble", Decimal("1299.00")),
    ("krzesło", "meble", Decimal("899.00")),
]
REGIONS = ["Warszawa", "Kraków", "Wrocław", "Gdańsk", "Poznań"]  # ta sama lista co w regule known_region
ORDERS_PER_BATCH = 200

rng = random.Random()
batch_id = str(uuid.uuid4())  # jedna partia na jeden run zadania
ingested_at = datetime.now(timezone.utc)
day_start = datetime(run_date.year, run_date.month, run_date.day, tzinfo=timezone.utc)


def make_order():
    """Jedno poprawne zamówienie z losową chwilą w dniu run_date."""
    product, category, unit_price = rng.choice(PRODUCTS)
    return {
        "order_id": str(uuid.uuid4()),
        "order_ts": day_start + timedelta(seconds=rng.randrange(24 * 60 * 60)),
        "customer_id": f"C{rng.randint(1, 500):04d}",
        "product": product,
        "category": category,
        "quantity": rng.randint(1, 3),
        "unit_price": unit_price,
        "region": rng.choice(REGIONS),
        "batch_id": batch_id,
        "ingested_at": ingested_at,
    }


orders = [make_order() for _ in range(ORDERS_PER_BATCH)]
orders += [
    {**make_order(), "customer_id": None},   # brak klienta: pipeline odrzuci wiersz
    {**make_order(), "quantity": 0},         # ilość 0: pipeline odrzuci wiersz
    {**make_order(), "quantity": -2},        # ilość ujemna: pipeline odrzuci wiersz
    {**make_order(), "region": "Szczecin"},  # region spoza listy: wiersz zostaje, reguła go policzy
]

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table} (
  order_id STRING,
  order_ts TIMESTAMP,
  customer_id STRING,
  product STRING,
  category STRING,
  quantity INT,
  unit_price DECIMAL(10, 2),
  region STRING,
  batch_id STRING,
  ingested_at TIMESTAMP
)
COMMENT 'Surowe zamówienia. Zadanie ingest_orders tylko dopisuje wiersze, bo orders_clean czyta je strumieniowo.'
""")

df = spark.createDataFrame(orders, schema=spark.table(table).schema)
df.write.mode("append").saveAsTable(table)

# ID partii dla zadania check_quality, które sprawdza tylko tę partię.
dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)
print(f"{table}: dopisano {len(orders)} wierszy z dnia {run_date} (partia {batch_id}).")
