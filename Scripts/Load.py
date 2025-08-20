# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Leemos la data de la capa silver
df_ecommerce = spark.table("catalog_dev.silver.ecommerce_transformed")

# COMMAND ----------

df_ecommerce.limit(5).display()

# COMMAND ----------

df_summary_product_category = df_ecommerce.groupBy("Product_Category").agg(round(avg("Price"), 2).alias("Avg_Price")\
                                                                             ,round(avg("Discount"), 2).alias("Avg_Discount")\
                                                                             ,sum("Units_Sold").alias("Total_Units_Sold")\
                                                                             ,round(sum("Revenue"), 2).alias("Total_Revenue"))

# COMMAND ----------

df_summary_product_category.display()

# COMMAND ----------

df_summary_customer_segment = df_ecommerce.groupBy("Customer_Segment").agg(round(avg("Revenue"), 2).alias("Avg_Revenue")\
                                                                           ,round(avg("Marketing_Spend"), 2).alias("Avg_Marketing_Spend")\
                                                                            ,round(sum("Revenue")/sum("Marketing_Spend"), 2).alias("ROI"))

# COMMAND ----------

df_summary_customer_segment.display()

# COMMAND ----------

df_summary_product_category.printSchema()

# COMMAND ----------

df_summary_customer_segment.printSchema()

# COMMAND ----------

# Almaenamos en la capa Golden
df_summary_product_category.write.mode("overwrite").format("delta").saveAsTable("catalog_dev.gold.summary_product_category")
df_summary_customer_segment.write.mode("overwrite").format("delta").saveAsTable("catalog_dev.gold.summary_customer_segment")

# COMMAND ----------

