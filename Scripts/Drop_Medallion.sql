-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("storage-name", "adlsfinalproject1")
-- MAGIC dbutils.widgets.text("container", "data")
-- MAGIC
-- MAGIC storage_name = dbutils.widgets.get("storage-name")
-- MAGIC container = dbutils.widgets.get("container")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("storage_path", f"abfss://{container}@{storage_name}.dfs.core.windows.net")
-- MAGIC dbutils.widgets.text("catalog", "catalog_dev")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC path = dbutils.widgets.get("storage_path")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Eliminacion de las tablas Bronze**

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS catalog_dev.bronze.ECOMMERCE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Quitamos la data de la direccion fisica (bronze)
-- MAGIC dbutils.fs.rm(f"{path}/bronze/ECOMMERCE", True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Eliminacion de las tablas Silver**

-- COMMAND ----------

DROP TABLE IF EXISTS catalog_dev.silver.ECOMMERCE_TRANSFORMED;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Quitamos la data de la direccion fisica (silver)
-- MAGIC dbutils.fs.rm(f"{path}/silver/ECOMMERCE_TRANSFORMED", True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Eliminacion de las tablas Golden**

-- COMMAND ----------

DROP TABLE IF EXISTS catalog_dev.gold.SUMMARY_CUSTOMER_SEGMENT;
DROP TABLE IF EXISTS catalog_dev.gold.SUMMARY_PRODUCT_CATEGORY;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Quitamos la data de la direccion fisica (silver)
-- MAGIC dbutils.fs.rm(f"{path}/gold/SUMMARY_CUSTOMER_SEGMENT", True)
-- MAGIC dbutils.fs.rm(f"{path}/gold/SUMMARY_PRODUCT_CATEGORY", True)
