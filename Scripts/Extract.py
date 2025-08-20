# Databricks notebook source
# Importamos librerias
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Definimos los path de la data que se va usar
ecommerce_data = "abfss://data@adlsfinalproject1.dfs.core.windows.net/Ecommerce_Sales_Prediction_Dataset.csv"

# COMMAND ----------

# Esquema de la data de ecommerce
ecommerce_schema = StructType([
  StructField("Date", StringType(), nullable=True),
  StructField("Product_Category", StringType(), nullable=True),
  StructField("Price", DoubleType(), nullable=True),
  StructField("Discount", DoubleType(), nullable=True),
  StructField("Customer_Segment", StringType(), nullable=True),
  StructField("Marketing_Spend", DoubleType(), nullable=True),
  StructField("Units_Sold", IntegerType(), nullable=True)
  ])

# COMMAND ----------

# Leemos la data
df_ecommerce = spark.read.schema(ecommerce_schema).csv(ecommerce_data, header=True, sep=',')

# COMMAND ----------

#df_ecommerce = df_ecommerce.withColumn("Date", date_format(col("Date"), "yyyy-MM-dd"))
from pyspark.sql.functions import to_date
df_ecommerce = df_ecommerce.withColumn("Date", to_date("Date", "dd-MM-yyyy"))

# COMMAND ----------

df_ecommerce.limit(10).display()

# COMMAND ----------

df_ecommerce.printSchema()

# COMMAND ----------

# Almacenamos la data en formato delta
df_ecommerce.write.mode("overwrite").format("delta").saveAsTable("catalog_dev.bronze.ecommerce")

# COMMAND ----------

