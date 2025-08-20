# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col

# COMMAND ----------

# Leemos la data de la capa bronze
df_ecommerce = spark.table("catalog_dev.bronze.ecommerce")

# COMMAND ----------

df_ecommerce.display()

# COMMAND ----------

# Creamos una vista temporal de la tabla para realizar transformaciones
df_ecommerce.createOrReplaceTempView("ecommerce_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ecommerce_delta LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verfiicamos si hay valores vacios en la tabla
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN Date IS NULL THEN 1 ELSE 0 END) AS null_Date,
# MAGIC   SUM(CASE WHEN Product_Category IS NULL THEN 1 ELSE 0 END) AS null_Product_Category,
# MAGIC   SUM(CASE WHEN Price IS NULL THEN 1 ELSE 0 END) AS null_Price,
# MAGIC   SUM(CASE WHEN Discount IS NULL THEN 1 ELSE 0 END) AS null_Discount,
# MAGIC   SUM(CASE WHEN Customer_Segment IS NULL THEN 1 ELSE 0 END) AS null_Customer_Segment,
# MAGIC   SUM(CASE WHEN Marketing_Spend IS NULL THEN 1 ELSE 0 END) AS null_Marketing_Spend,
# MAGIC   SUM(CASE WHEN Units_Sold IS NULL THEN 1 ELSE 0 END) AS null_Units_Sold
# MAGIC FROM
# MAGIC   ecommerce_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificamos si hay filas duplicadas
# MAGIC SELECT *, COUNT(*) as count FROM ecommerce_delta GROUP BY Date, Product_Category, Price, Discount, Customer_Segment, Marketing_Spend, Units_Sold 
# MAGIC HAVING count > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Agregamos la columna Revenue
# MAGIC SELECT *, ROUND(Price * Units_Sold, 2) AS Revenue FROM ecommerce_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Agregamos la columna de descuentio efectivo en dinero y la columna del Precio Final luego de aplicar el descuento
# MAGIC SELECT *, ROUND((Price * Discount)/100, 2) AS Discount_Amount, ROUND((Price - Discount_Amount), 2) AS Final_Price FROM ecommerce_delta; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Agregamos la columna para el nivel de ventas
# MAGIC SELECT *, 
# MAGIC CASE  WHEN Units_Sold > 30 THEN 'Alta'
# MAGIC       WHEN Units_Sold > 20 THEN 'Media'
# MAGIC       ELSE 'Baja' 
# MAGIC END AS Sale_Level FROM ecommerce_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Agrupamos todo en una solo query
# MAGIC SELECT *,
# MAGIC   ROUND(Price * Units_Sold, 2) AS Revenue,
# MAGIC   ROUND((Price * Discount)/100, 2) AS Discount_Amount, 
# MAGIC   ROUND((Price - Discount_Amount), 2) AS Final_Price,
# MAGIC   CASE  WHEN Units_Sold > 30 THEN 'Alta'
# MAGIC       WHEN Units_Sold > 20 THEN 'Media'
# MAGIC       ELSE 'Baja' 
# MAGIC   END AS Sale_Level
# MAGIC FROM ecommerce_delta; 

# COMMAND ----------

# Almacenamos el resultado de la query en un dataframe
query = '''
        SELECT *,
          ROUND(Price * Units_Sold, 2) AS Revenue,
          ROUND((Price * Discount)/100, 2) AS Discount_Amount, 
          ROUND((Price - Discount_Amount), 2) AS Final_Price,
          CASE  WHEN Units_Sold > 30 THEN 'Alta'
              WHEN Units_Sold > 20 THEN 'Media'
              ELSE 'Baja' 
          END AS Sale_Level
        FROM ecommerce_delta; 
'''

df_ecommerce_transformed = spark.sql(query)

# COMMAND ----------

df_ecommerce_transformed.limit(10).display()

# COMMAND ----------

# Almacenamos la transformacion en la capa Silver
df_ecommerce_transformed.write.mode("overwrite").format("delta").saveAsTable("catalog_dev.silver.ecommerce_transformed")

# COMMAND ----------

