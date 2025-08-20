-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

create widget text storage_path default 'abfss://data@adlsfinalproject1.dfs.core.windows.net';
create widget text catalogo default 'catalog_dev';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Creacion del Catalog**

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS catalog_dev;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Creacion de Schemas**

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS catalog_dev.bronze;
CREATE SCHEMA IF NOT EXISTS catalog_dev.silver;
CREATE SCHEMA IF NOT EXISTS catalog_dev.gold;

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

USE CATALOG ${catalogo};

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Creacion de tablas Bronze**

-- COMMAND ----------

CREATE OR REPLACE TABLE catalog_dev.bronze.ECOMMERCE(
 Date DATE, Product_Category STRING, Price DOUBLE, Discount DOUBLE, Customer_Segment STRING,
 Marketing_Spend DOUBLE, Units_Sold INT
)
USING DELTA
LOCATION 'abfss://data@adlsfinalproject1.dfs.core.windows.net/bronze/ECOMMERCE';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Creacion Tablas Silver**

-- COMMAND ----------

CREATE OR REPLACE TABLE catalog_dev.silver.ECOMMERCE_TRANSFORMED(
 Date DATE, Product_Category STRING, Price DOUBLE, Discount DOUBLE, Customer_Segment STRING,
 Marketing_Spend DOUBLE, Units_Sold INT, Revenue DOUBLE, Discount_Amount DOUBLE, 
 Final_Price DOUBLE, Sale_Level STRING
)
USING DELTA
LOCATION 'abfss://data@adlsfinalproject1.dfs.core.windows.net/silver/ECOMMERCE_TRANSFORMED';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## **Creacion Tablas Gold**

-- COMMAND ----------

CREATE OR REPLACE TABLE catalog_dev.gold.SUMMARY_PRODUCT_CATEGORY(
  Product_Category STRING, Avg_Price DOUBLE, Avg_Discount DOUBLE, 
  Total_Units_Sold LONG, Total_Revenue DOUBLE
)
USING DELTA
LOCATION 'abfss://data@adlsfinalproject1.dfs.core.windows.net/gold/SUMMARY_PRODUCT_CATEGORY';

CREATE OR REPLACE TABLE catalog_dev.gold.SUMMARY_CUSTOMER_SEGMENT(
  Customer_Segment STRING, Avg_Revenue DOUBLE, Avg_Marketing_Spend DOUBLE, 
  ROI DOUBLE
)
USING DELTA
LOCATION 'abfss://data@adlsfinalproject1.dfs.core.windows.net/gold/SUMMARY_CUSTOMER_SEGMENT';

-- COMMAND ----------

