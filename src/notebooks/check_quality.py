# Databricks notebook source
# MAGIC %md
# MAGIC ## check_quality: sprawdzenie wyniku runu
# MAGIC Zadanie sprawdza partię z tego runu i podsumowanie dnia z pipeline'u. Gdy warunek nie jest spełniony,
# MAGIC rzuca wyjątek: zadanie i cały run kończą się wtedy błędem.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "szkolenie")
dbutils.widgets.text("schema", "demo_dev")
dbutils.widgets.text("run_date", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date")
# ID partii z zadania ingest_orders. Poza jobem (ręczne uruchomienie) pusty tekst i sprawdzenie partii jest pomijane.
batch_id = dbutils.jobs.taskValues.get(taskKey="ingest_orders", key="batch_id", debugValue="")


def table(name):
    return spark.table(f"{catalog}.{schema}.{name}")


errors = []

# COMMAND ----------

# 1. Partia z tego runu przeszła przez pipeline.
if batch_id:
    raw_rows = table("raw_orders").where(F.col("batch_id") == batch_id).count()
    clean_rows = table("orders_clean").where(F.col("batch_id") == batch_id).count()
    print(f"Partia {batch_id}: raw_orders {raw_rows}, orders_clean {clean_rows}, odrzucone {raw_rows - clean_rows}")
    if clean_rows == 0:
        errors.append("orders_clean nie ma wierszy z tej partii")

# 2. W orders_clean nie ma wierszy, które reguły z ON VIOLATION DROP ROW miały odrzucić.
broken = table("orders_clean").where("customer_id IS NULL OR quantity <= 0").count()
if broken:
    errors.append(f"orders_clean ma {broken} wierszy, które reguły jakości powinny odrzucić")

# 3. Podsumowanie dnia istnieje i ma dodatnią sprzedaż.
daily = table("daily_sales")
if run_date:
    daily = daily.where(F.col("order_date") == F.lit(run_date).cast("date"))
day_rows = daily.collect()
if not day_rows:
    errors.append(f"daily_sales nie ma wiersza dla dnia {run_date or '(brak run_date)'}")
elif any(row.total_amount <= 0 for row in day_rows):
    errors.append("daily_sales ma dzień ze sprzedażą 0 albo ujemną")

# COMMAND ----------

display(daily)

if errors:
    raise AssertionError("Sprawdzenie jakości nie przeszło:\n- " + "\n- ".join(errors))
print("Sprawdzenie jakości przeszło.")
